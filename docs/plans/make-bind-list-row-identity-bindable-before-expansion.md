# Make BindList row identity bindable before expansion

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket: `ticket_1785436979_640117`; run
  `run_1785436979_236604`.
- Repository routing was resolved from the admitted Botster spawn-target
  registry, not from the process working directory. The assigned worktree is
  clean at `95e829ad58f772f88bafdc1f8ada56998fb63503`, the current
  `trybotster/botster-hub` main commit at Plan time.
- Role and repository playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], and [[botster-hub-playbook]].
- Surface guidance: [[botster-hub-client-playbook]] for generated client
  artifacts and compatibility metadata; [[botster-package-reviewer-playbook]]
  and [[botster-package-verifier-playbook]] for the producer-owned package
  fixture, publication, and independent consumer proof.
  [[project-pipelines-playbook]] is intentionally not loaded because this
  ticket changes neither Project Pipelines package/plugin paths nor workflow
  policy.
- Architecture maps and targeted notes: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[ui contract row ids can bind before template expansion]],
  [[plugin dynamic ui lists bind to plugin-owned entities]],
  [[ui bind list where filters plugin entity rows before template expansion]],
  [[ui bind list empty template renders entity backed empty rows]],
  [[ui bind list typed templates are narrower than the runtime wire grammar]],
  [[botster package surface semantics live in ui contract while hub owns admission]],
  [[cross-client ui should share semantic primitives and actions with renderer-specific adapters]],
  [[botster-web should import canonical core uinode fixtures instead of mirroring them]],
  [[plugin surface handlers must validate against hub locked uinode contract]],
  [[plugin surface requests require a declared id and operation]],
  [[published fixture readmes are part of the shipped contract]],
  [[conformance fixture revisions must be unique per published content]],
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[webrtc bootstrap origin must be requested after the package server binds]],
  [[plugin worker queue capacity and executor concurrency are independent host profile knobs]],
  [[durable state version preflight must precede shape deserialization after cold turkey changes]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[adding a hub client feature constant is a three site change]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  [[generated typescript dtos must encode serde field optionality]],
  [[botster is a lua plugin platform not an agent tool]],
  [[botster plugin runtime uses supervisor plus per plugin workers]],
  [[plugin mcp handlers run in plugin worker vms]],
  [[botster plugins need headless real-runtime test harnesses]],
  [[plugin conformance packages prove shared contracts while examples prove product behavior]],
  and [[package owned pipeline reconciliation preserves device local agent selection]].
- Project Pipelines policy notes required by the planner role were also
  consulted for workflow discipline; they do not expand implementation scope:
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]], and
  [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repository evidence inspected: root, UI-contract, and Hub test-support
  READMEs; workspace/package manifests; the authored Rust UiNode, UiNodeId,
  UiBind, and UiBindList types and validation; generated JSON Schema,
  TypeScript declarations, and conformance fixtures; the canonical
  `contract.sessions` Lua fixture and its crate/package mirrors; Rust and Node
  session-binding reference materializers; the exact isolated-Hub/plugin-worker
  conformance runner; daemon lifecycle assertions; package synchronization and
  publication scripts; CI/test entry points; and prior repository plan/report
  artifacts.
- Downstream code was inspected read-only on the active
  `botster-tui` consumer ticket branch. Its production resolver expands
  BindList props and children but cannot resolve `UiNode.id`; it therefore
  rejects a multi-row template containing a literal id. Focus, keyboard
  dispatch, and action state subsequently key on the realized string node id.
  This confirms the runtime seam without moving renderer behavior into Hub.
- Registry preflight on 2026-07-30: public latest is
  `@trybotster/ui-contract@0.1.1` and
  `@trybotster/hub-test-support@0.1.16`, matching the checked-in package
  coordinates. Implementation must repeat this check immediately before
  assigning immutable versions. Expected coordinates are UI contract `0.2.0`
  (the Rust-authored node field type changes) and Hub test support `0.1.17`;
  those numbers are not authoritative until the recheck.
- Plan Review `review_1785438421_704974` returned six specification gaps. This
  revision reconciles all six against current code: it pins root versus
  BindList-item validation semantics across both public validation entry
  points; records the read-only TUI-kit helper audit; adds `src/runtime.rs`
  render and action-replacement admission controls; preserves the three
  existing published materializer signatures while adding a named row/payload
  API; defines all five stage expectations; switches the oracle to a
  required-identity button; and changes the bound value rule from non-empty to
  non-blank.

## Contract decision

Add a distinct **authored node identity** value for `UiNode.id`: either a
realized literal `UiNodeId` or the existing canonical `UiBind` sentinel.
Do not widen `UiNodeId` itself. `UiActionRequest.node_id`,
`UiActionResult.node_id`, focus maps, and dispatched identities remain realized
strings.

The only bound-id form is the existing row-relative sentinel:

```json
{ "id": { "$bind": "@/session_uuid" } }
```

It is valid only inside a `UiBindList.item_template`, where a current producer
row exists. Resolve it after `where` filtering and before the expanded UiNode
enters normal renderer/focus/action handling. The referenced row field must be
a non-blank JSON string after trimming, matching the existing
`validate_stable_id` rule. There is no coercion, interpolation, concatenation,
fallback, row index, or client-local synthetic id. Absolute paths, unresolved
paths, non-string values, empty or whitespace-only strings, and bound ids in
roots, ordinary static children, or `empty_template` are contract errors.
Literal ids remain wire compatible.

After materialization the id is an ordinary `UiNodeId`. Duplicate realized ids
within one rendered tree are producer/contract errors; they are never repaired
by a client. The canonical multi-row oracle binds `@/session_uuid`, whose
producer-owned uniqueness already matches the `/session` row identity.

## Scope

1. **Express and validate authored identity in the canonical UI contract.**
   - Add the narrow Rust authored-id enum/value and use it only for `UiNode.id`.
     Preserve the stable realized `UiNodeId` newtype for request/result and
     runtime identity.
   - Preserve the public root-entry semantics of `UiNode::validate()`,
     `validate_ui_node()`, and `validate_ui_node_with_capabilities()`: each
     treats the supplied node as a root with no current row and rejects a
     bound root id. Thread a private validation context through the recursive
     semantic validator. Ordinary children, slots, conditionals, `bind_if`
     nodes, and `empty_template` retain static context; only the descent from
     `validate_bind_list` into `item_template` switches to BindList-row
     context. Capability validation continues after that semantic pass and
     does not relax identity.
   - Do not add a permissive public detached-template validator. Consumers
     validate the complete authored tree, materialize a selected item into a
     literal-id node, and may then validate that realized subtree through the
     existing root entry points. Reuse the existing `UiBind` grammar; do not
     create a second binding language.
   - Regenerate JSON Schema and TypeScript so only `UiNodeBase.id` accepts
     `UiNodeId | UiBind`; action request/result `node_id` stays `UiNodeId`.
     Add positive literal/bound round trips plus negative contextual,
     absolute, missing, empty, whitespace-only, and non-string materialization
     cases.
   - Add a checked conformance fixture whose pre-change parse/validation fails
     and whose post-change parse/validation succeeds. Document author-time
     versus realized identity and duplicate-id failure semantics.

2. **Add a producer-backed multi-row oracle to `contract.sessions`.**
   - Keep the existing exact-filter lifecycle controls: they prove current,
     ended, indeterminate, missing, patch, remove, and reconnect behavior.
   - Add one BindList under the same surface with
     `where.lifecycle_class = "current"`. Its item template is a `button`, so
     the real path exercises the required stable-id branch. Bind its id
     directly to `@/session_uuid`, give it a literal non-blank label, and reuse
     the already-declared `contract.action` descriptor. Its action payload
     carries a row-relative `session_uuid` bind so downstream keyboard
     dispatch can prove the selected row's id and payload without inventing a
     local fixture or adding a second action handler/protocol.
   - Pin the row-id oracle at every existing scenario stage, preserving
     producer frame order:
     `initial = ["session-transition", "session-stable-current"]`;
     `after_ended_patch`, `after_indeterminate_patch`, `after_remove`, and
     `after_reconnect = ["session-stable-current"]`. The transition row stops
     matching after the first lifecycle patch and remains absent after remove
     and reconnect.
   - Preserve the published lifecycle materializer APIs and return shapes:
     Rust
     `materialize_session_plugin_bindings(&Value, &[DaemonEntityFrame]) ->
     Result<BTreeMap<String, String>, String>`, Node
     `materializeSessionPluginBindings(surface, frames) ->
     Record<string, string>`, and
     `materializeSessionPluginBindingScenario(scenario) ->
     Record<string, Record<string, string>>`. Teach them to identify and
     validate the exact-filter lifecycle controls while ignoring the
     separately identified row-id oracle.
   - Add an additive published row materialization type with
     `{ node_id, action_payload }`, plus sibling APIs: Rust
     `materialize_session_plugin_rows(&Value, &[DaemonEntityFrame]) ->
     Result<Vec<SessionPluginMaterializedRow>, String>`, Node
     `materializeSessionPluginRows(surface, frames) ->
     Array<{ node_id: string; action_payload: unknown }>`, and Node
     `materializeSessionPluginRowScenario(scenario) ->
     Record<string, Array<{ node_id: string; action_payload: unknown }>>`.
     They locate exactly one canonical current-row oracle, apply the same
     public frames, resolve the id and bound payload from each selected
     producer row, require non-blank ids, retain producer order, and reject
     duplicates. The canonical payload is
     `{ operation: "select_session", session_uuid: "<realized row id>" }`.
     Do not merely inspect that `$bind` appears in JSON. This additive API
     supports the planned Hub-test-support patch release without breaking the
     two open consumers.
   - Edit the repo-root
     `fixtures/plugins/plugin-contract-matrix` source, then synchronize the
     byte-identical crate and generated npm mirrors. Update all fixture and
     package READMEs that currently say `contract.sessions` contains only one
     exact-filter BindList per reference.

3. **Prove the real Hub producer/admission path.**
   - Render `contract.sessions` through the installed package registry,
     supervisor, Lua plugin worker, surface route, Hub validation, client API,
     and daemon response path in the existing exact isolated-Hub runner.
   - Require the returned validated body to match the source-derived scenario,
     contain the canonical bound-id template, and materialize the two distinct
     ids against public `DaemonEntityFrame` rows. The structural equality check
     alone is insufficient.
   - Exercise both Hub admission roots in `src/runtime.rs`: the real render
     response must accept the bound id only inside the BindList item template,
     while a bound id on the rendered root or static child must fail. An
     otherwise accepted `UiActionResult.replacement` whose root or static child
     uses a bound id must also fail through action-result admission. Keep the
     existing inline binding-family admission tests green.
   - Extend the public conformance report and daemon lifecycle assertions with
     the multi-row oracle. This is the production-entry-point proof: a real
     plugin worker authors the sentinel, Hub admission accepts it, and the
     published reference materializer applies it to producer-backed rows.

4. **Advance and publish the normal contract artifacts.**
   - Advance the conformance fixture revision because checked fixture bytes and
     meaning change (expected 24 -> 25 after a collision check). Keep daemon
     protocol version 4 and add no feature constant: request framing and daemon
     event DTO shape are unchanged.
   - Regenerate every UI-contract and Hub test-support schema, declaration,
     fixture, metadata file, checksum, README coordinate, crate version,
     package version, dependency pin, and lockfile from its authoritative
     source. Generated npm fixture mirrors are never edited independently.
   - From a clean committed tree, use the repository publication script to
     pack UI contract first, install that exact tarball into Hub test support,
     and pack test support. Smoke-test those tarballs in fresh Rust/Node
     consumers before publication. If npm credentials or 2FA prevent the final
     publish, record the exact operator command and verified tarball
     coordinates; do not substitute a path dependency or unpublished local
     override for downstream work.

5. **Hand the canonical seam to the downstream owner.**
   - After merge/publication, downstream `botster-tui` ticket
     `ticket_1785298229_854008` must repin the merged Hub contract, remove its
     explicit multi-row/literal-id safety rejection, resolve the authored id
     before expansion enters renderer state, and prove that focusing and
     keyboard-activating row 2 of the canonical button oracle dispatches
     `session-stable-current` and its row-bound payload.
   - That child also owns the required real Workspaces surface proof. If a
     reusable renderer primitive must change in `botster-tui-kit`, register a
     separately routed dependency against the kit target; do not edit it from
     this Hub run.

## Non-scope

- No edits to `botster-tui`, `botster-tui-kit`, `botster-web`,
  `botster-workspaces`, `botster-core`, or Project Pipelines source.
- No client-local synthetic ids, row-index ids, concatenation grammar,
  fallback ids, implicit string coercion, or collision repair.
- No widening of realized `UiNodeId` or acceptance of bind sentinels in action
  request/result `node_id`.
- No generalized template expression language, typed-template cleanup, entity
  query redesign, new renderer, new action, broad UiNode refactor, or adjacent
  cleanup.
- No daemon protocol-version bump, compatibility feature, subscription change,
  terminal data-plane change, or new Hub entity family.
- No standalone publication-only ticket. Publication remains the final
  operator step of the normal verified package release if credentials require
  human intervention.

## Repository ownership boundaries and cross-repository dependencies

- **In-repository `botster-ui-contract`:** owns authored UiNode grammar,
  validation, schema, TypeScript declarations, conformance fixtures, and the
  realized-versus-authored identity distinction. It must remain
  renderer-neutral.
- **botster-hub:** owns plugin-surface admission and the real producer/runtime
  conformance path. It validates the authored contract but does not implement
  renderer focus or synthesize identity.
- **In-repository `botster-hub-test-support`:** owns the source-derived
  `/session` scenario, Rust/Node reference materializers, public report, exact
  runtime runner, generated package mirror, and downstream-consumable oracle.
- **In-repository `botster-hub-client`:** owns compatibility metadata. Only the
  conformance fixture revision changes; protocol version and feature set do
  not.
- **botster-core:** remains the lockfile-pinned runtime/plugin host dependency.
  This plan requires no Core API change. If Hub validation cannot admit the
  authored sentinel through the pinned public APIs, stop and register a
  prerequisite against the botster-core target instead of forking a private
  validator.
- **botster-tui:** downstream consumer ticket
  `ticket_1785298229_854008` is unblocked by this producer ticket. It owns
  expansion, focus, keyboard dispatch, and action-state proof.
- **botster-workspaces:** downstream product consumer owns its surface and
  row actions. It consumes the generic TUI behavior; Hub must not acquire
  Workspaces policy.
- **botster-tui-kit:** continues to own generic focus/input primitives. Its
  current `assert_custom_fallbacks_resolve` helper validates each complete
  fixture through `validate_ui_node_with_capabilities`, then recursively
  validates only static custom fallbacks; it does not revalidate every
  BindList item template as a detached root. The canonical button oracle
  therefore does not require a kit change. The downstream repin must run the
  kit conformance suite to prove that claim. If the actual repin reveals a
  detached-template caller or other required kit edit, create and register a
  separately routed kit dependency against
  `tgt_3dfae49c02454037bf13554f552baf7f` before changing kit code.

No prerequisite dependency is currently required for this Hub producer run.
The read-only kit audit above makes that a verified claim rather than an
assumption. The known dependency direction is downstream: TUI and Workspaces
wait for the merged/published Hub contract.

## Assumptions and unknowns

- The ticket means a directly bound row field, not an interpolated id format.
  `@/session_uuid` is sufficient for the canonical oracle and is the smallest
  grammar change.
- Authored bound ids are admitted only where BindList supplies a row. Allowing
  them in `empty_template` or a root would leave no deterministic
  materialization context.
- Existing public validation functions keep root semantics; the context is an
  internal recursive concern, not an ambient or caller-defaulted flag. A
  detached authored item template with a bound root id is intentionally
  invalid until it is materialized.
- Duplicate-id rejection may live in the generic client tree
  materialization/validation boundary rather than producer admission, because
  Hub does not possess the renderer's entity rows when it validates the
  authored tree. The canonical reference materializer must nevertheless make
  the error semantics executable and unambiguous.
- The UI contract version is expected to become `0.2.0` because changing the
  public Rust `UiNode.id` field type is source-breaking even though literal
  JSON remains compatible. Registry state at implementation time is
  authoritative.
- The next conformance revision is expected to be 25 and Hub test-support
  `0.1.17`; both require collision/registry preflight immediately before use.
- Final npm publication may require an operator credential/2FA step. Packing,
  exact-tarball installation, smoke proof, and publication coordinates remain
  required before the downstream ticket repins.
- No semantic choice is being silently waived. If implementation discovers
  that the existing `$bind` path grammar cannot distinguish author-time
  validation from row-time resolution without accepting unresolved root ids,
  stop and ask a human rather than broadening the grammar.

## Affected surfaces and files

- Plan: `docs/plans/make-bind-list-row-identity-bindable-before-expansion.md`.
- Authored UI contract:
  `crates/botster-ui-contract/src/lib.rs`,
  `crates/botster-ui-contract/tests/ui_contract_test.rs`,
  `crates/botster-ui-contract/src/assets.rs`,
  `crates/botster-ui-contract/tests/generated_assets_test.rs`,
  `crates/botster-ui-contract/Cargo.toml`.
- Generated/published UI package:
  `packages/ui-contract/index.d.ts`, `schema.json`,
  `conformance-fixtures.json`, `index.js`, `test.mjs`, `README.md`, and
  `package.json`.
- Canonical producer fixture:
  `fixtures/plugins/plugin-contract-matrix/plugin.lua` and `README.md`;
  synchronized crate mirror under
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix`; generated
  npm mirror under
  `packages/hub-test-support/fixtures/plugin-contract-matrix`.
- Hub reference/runtime conformance:
  `crates/botster-hub-test-support/src/lib.rs`,
  its fixture/metadata generator examples as implicated by regeneration,
  `packages/hub-test-support/index.js`, `index.d.ts`, `test.mjs`, `README.md`,
  `session-plugin-binding-conformance-fixture.json`, `metadata.json`,
  `first-party-client-support-matrix.json`, and `package.json`;
  `src/runtime.rs`; `tests/hub_daemon_lifecycle_test.rs`.
- Compatibility/versioning:
  `crates/botster-hub-client/src/lib.rs`, generated compatibility artifacts,
  affected crate manifests, root/public protocol documentation if it describes
  the identity grammar, and `Cargo.lock`.

This list names expected touch points, not permission for blanket rewrites.
Every changed line must trace to authored-id grammar, the multi-row oracle,
generated artifact/version consistency, documentation made stale by those
changes, or proof of the real runtime path.

## Risks and controls

- **Realized identity accidentally widened:** keep `UiNodeId` string-only and
  add a separate authored-id type; assert action request/result schemas and
  TypeScript reject `$bind`.
- **Unresolved bindings admitted:** preserve public root semantics, use a
  private recursive template context, and add contract plus Hub render/action
  replacement root/static/empty/absolute negative tests.
- **Coercion or collisions hide producer defects:** accept only non-blank JSON
  strings and reject duplicate realized ids; never add fallback/index logic.
- **Fixture looks correct but runtime is unwired:** require the exact
  isolated-Hub/plugin-worker runner and report fields, not only static asset
  assertions.
- **Generated/source drift:** edit the repo-root fixture and Rust generator
  sources, run check-mode generators, and require byte-equality tests for both
  mirrors and packed artifacts.
- **Immutable revision/version collision:** repeat npm and conformance
  preflights before assigning coordinates; never reuse published bytes.
- **Downstream claims success without the user path:** keep producer proof and
  TUI keyboard/action proof distinct; the TUI child must repin the merged
  contract and exercise the real Workspaces surface.
- **Published materializer break under a patch release:** retain all existing
  Rust/Node signatures and return shapes; add separately named row-and-payload
  materializers and test both APIs against the augmented surface.
- **Detached-template validation regresses TUI-kit:** retain root semantics,
  document that authored item templates validate only through their containing
  BindList, and run the kit conformance suite at the downstream repin.
- **Scope creep into a new expression/query system:** support only the existing
  row-relative `$bind` sentinel and direct row field.

## Acceptance checks and tests

1. **Contract red/green and negative controls**
   - Record that the bound-id conformance case fails against the pre-change
     parser/validator and passes after the authored-id change.
   - Rust round trips literal and bound authored ids. Schema/TypeScript permit
     `$bind` only in `UiNode.id`; action request/result `node_id` remains a
     string.
   - Reject absolute, missing, empty, whitespace-only, non-string, root/static,
     `empty_template`, and duplicate-materialized id cases.
   - Assert `UiNode::validate()`, `validate_ui_node()`, and
     `validate_ui_node_with_capabilities()` reject the same detached bound-id
     root, while a containing BindList validates its item template.
   - Run:

     ```sh
     ./test.sh -p botster-ui-contract
     npm --prefix packages/ui-contract run check
     npm --prefix packages/ui-contract test
     ```

2. **Producer-backed oracle parity**
   - Existing Rust/Node lifecycle materializer signatures and output shapes
     remain compatible against the augmented surface.
   - New Rust/Node row materializers consume the checked scenario and public
     session rows. Their `node_id` projections are
     `["session-transition", "session-stable-current"]` initially and only
     `["session-stable-current"]` after each of the four subsequent stages;
     each returned row also contains
     `{ operation: "select_session", session_uuid: node_id }`.
   - Lifecycle exact-filter stages continue to prove present, missing, patch,
     remove, and reconnect behavior.
   - Source/crate/npm fixture trees and generated metadata remain byte-current.
   - Run:

     ```sh
     ./test.sh -p botster-hub-test-support
     npm --prefix packages/hub-test-support run check
     npm --prefix packages/hub-test-support test
     ```

3. **Actual runtime path**
   - Build the lockfile-pinned worker and run the exact isolated-Hub test:

     ```sh
     cargo build --locked -p botster-core --bin botster-session-worker
     ./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
     ```

   - Assert the real Lua worker response matches the source scenario, Hub
     validation accepts the authored bound id on the required-identity button,
     and the report exposes the two producer-materialized distinct ids and
     payloads. Through `src/runtime.rs`, reject the same id on a render
     root/static child and on an accepted action-result replacement root/static
     child. Static JSON existence is not proof.

4. **Workspace gates**

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   ./test.sh
   git diff --check
   ```

5. **Publication and independent consumers**
   - Recheck npm versions, commit all generated artifacts, and from a clean
     tree run `script/publish-npm-packages --dry-run`.
   - Install the exact UI tarball into Hub test support as the script does,
     then install both exact tarballs into fresh temporary consumers. Verify
     package versions/checksums/revision, schema/declaration imports, fixture
     loading, and the two realized ids. Where the repository toolchain permits,
     a strict TypeScript consumer must compile bound `UiNode.id` and fail a
     bound action `node_id`.
   - Publish UI contract before Hub test support. If credentials/2FA block the
     final publish, hand the operator the exact verified command and
     coordinates, then verify the public registry before downstream repinning.

6. **Required downstream proof**
   - In `ticket_1785298229_854008`, repin the merged/published Hub contract
     without a path override; materialize ids before renderer expansion; remove
     the ambiguity guard; render the two canonical matching buttons; focus row
     2; send keyboard activation; and assert
     `node_id = "session-stable-current"` plus that row's bound action payload.
     Run botster-tui-kit's conformance suite against the repinned contract to
     prove its custom-fallback helper remains valid. Exercise the owner-authored
     Workspaces surface, not only a unit fixture.
   - This proof is required before the cross-repository behavior is declared
     delivered, but its code and review remain in the TUI/Workspaces-owned
     targets.

## Vault gaps worth capturing

- [[ui contract row ids can bind before template expansion]] currently cites a
  legacy `trybotster` monolith implementation as if it were present authority,
  while the current Hub-owned `botster-ui-contract@0.1.1` still models
  `UiNode.id` as a plain optional string. After implementation, update that
  note to distinguish historical proof from the canonical Hub-owned landing.
- Add the durable distinction that authored `UiNode.id` may bind inside a row
  template while realized/action `node_id` remains a string. This prevents
  future schema generators from widening every identity-bearing field.
- Verification evidence should be appended only after the implementation,
  exact runtime test, tarball consumer smoke, and downstream repin exist.
  No durable vault content is captured during Plan because this artifact is a
  proposal, not implementation evidence.
