# Define bound identity for BindList item descendants

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket: `ticket_1785443253_376782`; run
  `run_1785602427_826142`.
- The target was resolved from the admitted Botster spawn-target registry, not
  from the ambient directory. The assigned worktree is clean at
  `88d343870700994d310f090fd5b2c4dbabb07405`, which matched `origin/main` at
  Plan time.
- Repository routing and role guidance: [[planner-playbook]],
  [[botster-planner-playbook]], and [[botster-hub-playbook]].
- Additional ownership/surface guidance: [[botster-hub-client-playbook]],
  [[botster-reviewer-playbook]], [[botster-runtime-reviewer-playbook]],
  [[botster-package-reviewer-playbook]], [[botster-verifier-playbook]], and
  [[botster-package-verifier-playbook]]. [[project-pipelines-playbook]] was not
  loaded because no Project Pipelines package/plugin path or workflow policy
  changes in this ticket; Project Pipelines is only the delivery mechanism.
- Architecture maps and required planner/Hub notes: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
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
  [[hub drain advances non attached session lifecycle]], and
  [[hub shutdown preserves durable session workers]].
- Targeted contract and verification notes:
  [[ui contract row ids can bind before template expansion]],
  [[renderer state accepts only realized literal identity]],
  [[post expansion identity uniqueness is scoped to one render not one tree]],
  [[ui bind list typed templates are narrower than the runtime wire grammar]],
  [[plugin dynamic ui lists bind to plugin-owned entities]],
  [[ui bind list where filters plugin entity rows before template expansion]],
  [[botster wire v2 clients must consume ui tree snapshots and render composites with entity stores]],
  [[acceptance harness region oracles must key on node identity not concatenated text]],
  [[hub support metadata can force a web ui contract cold update]],
  [[botster package surface semantics live in ui contract while hub owns admission]],
  [[cross-client ui should share semantic primitives and actions with renderer-specific adapters]],
  [[plugin surface handlers must validate against hub locked uinode contract]],
  [[published fixture readmes are part of the shipped contract]],
  [[conformance fixture revisions must be unique per published content]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  [[hub test support npm releases need external consumer smoke]], and
  [[plugin conformance packages prove shared contracts while examples prove product behavior]].
- Repository evidence inspected: root/package/fixture READMEs; workspace and
  npm manifests; `UiAuthoredNodeId`, `UiBindList`, contextual validation, asset
  generation, schema/TypeScript outputs, and contract tests; the canonical
  `contract.sessions` Lua producer; Rust and Node strict reference
  materializers; Hub render/action admission; isolated-Hub conformance and
  daemon lifecycle assertions; package sync/publication entry points; CI/test
  wrappers; and the preceding direct-root plan and implementation commits.
- Read-only downstream inspection confirmed that Web currently realizes only
  the direct item-template root in `IonicUiNodeRenderer.tsx`; TUI currently
  realizes only that root in `crates/botster-tui/src/app.rs`; and TUI-kit pins
  the current Hub UI-contract revision separately. TUI therefore needs a
  TUI-kit repin before it can converge on one new contract source.
- Registry preflight on 2026-08-01 found public latest
  `@trybotster/ui-contract@0.2.0` and
  `@trybotster/hub-test-support@0.1.18`, matching the checked-in coordinates.
  Implementation must repeat this immediately before assigning immutable
  versions and fixture revisions.
- Human answer `question_1785602687_603211` chose composed identity: derive
  every realized descendant id from the row's canonical bound identity plus a
  producer-authored literal control key, using a Hub-contract-owned,
  versioned, injective UTF-8 byte-length-prefixed encoding. It explicitly
  rejects separate presentation-specific id fields in entity records,
  descendant full-id `$bind`, client-local encoding, and compatibility grammar.

## Contract decision

Preserve the 0.2.0 direct-root form unchanged:

```json
{
  "type": "inline",
  "id": { "$bind": "@/session_uuid" }
}
```

Add exactly one new authored descendant-id form:

```json
{
  "type": "button",
  "id": {
    "$kind": "bind_list_descendant_id",
    "key": "remove"
  }
}
```

Name the Rust value `UiBindListDescendantId` and add it as a distinct
`UiAuthoredNodeId` variant. The TypeScript authored-id union and JSON Schema
use the same shape. The form is valid only below the direct item-template root
of a `UiBindList`, and only when that root uses the existing row-relative
`UiBind`. It always derives from the nearest enclosing BindList item root;
nested BindLists establish a new root context. It is invalid on the item root,
outside an item template, under `empty_template`, or below a literal/absent
item-root id. A descendant `{ "$bind": ... }` remains invalid; there is no
second way to supply a complete descendant id.

The literal `key` must be a non-blank string. Preserve its exact UTF-8 bytes
after validation; do not trim, normalize, case-fold, coerce, or restrict it to
ASCII. Every `bind_list_descendant_id` key must be unique across all
identity-bearing descendants in that authored item template, including slots,
conditionals, and bind-if branches. This ticket-specific authored-key rule is
deliberately stricter than render-scoped final-id uniqueness: it prevents two
controls in one template from claiming the same semantic discriminator before
client state chooses a branch.

After `where` filtering selects a row, resolve the direct root `$bind` to the
canonical non-blank string row id. Materialize each descendant id as:

```text
botster-ui-descendant-v1:<R>:<ROW><K>:<KEY>
```

`R` and `K` are canonical base-10 byte counts with no sign and no leading
zeros except the single digit `0`; lengths count UTF-8 bytes, not Unicode
scalar values or UTF-16 code units. `<ROW>` and `<KEY>` are copied byte for
byte. For row `session-1` and key `remove`, the result is
`botster-ui-descendant-v1:9:session-16:remove`. Consumers do not concatenate,
escape, hash, parse, or choose the prefix themselves. The Rust contract crate
and generated npm package expose canonical realization helpers, and all Hub
reference materializers and downstream consumers call those helpers.

The emitted value is an ordinary literal `UiNodeId`. React keys, DOM
`data-ui-node-id`, TUI focus/hit maps, and `UiActionRequest.node_id` receive
that exact string. Existing final-tree collision checks still reject a
generated id colliding with a root, static literal, sibling, or another row;
clients never repair a collision. Direct item-root ids remain the producer row
ids and are not rewritten. Literal descendant ids remain wire-valid for
existing single-row/static use, but repeated realized literals remain subject
to collision rejection.

## Scope

1. **Extend the Hub-owned authored identity grammar and validator.**
   - Add `UiBindListDescendantId` and the new `UiAuthoredNodeId` variant without
     widening literal `UiNodeId`, action request/result `node_id`, or any daemon
     DTO.
   - Replace the current three-state validation context with a context that
     carries the nearest item-root authored identity and one key set for the
     complete item template. The direct root continues to accept only the
     existing row-relative `$bind`; descendants accept literals or the new
     keyed form but reject full-id `$bind`.
   - Traverse children, slots, conditionals, and bind-if nodes consistently.
     A nested BindList validates in a fresh context. `empty_template` remains
     static. Reject absent/literal item-root sources, blank keys, duplicate
     keys anywhere in the authored template, and every misplaced new form.
   - Put the canonical encoding and realization helpers in
     `botster-ui-contract`. Rust and the generated JavaScript runtime must
     produce byte-identical strings for ASCII, delimiter-like, whitespace,
     multibyte, and emoji inputs. Do not make clients reimplement the formula.
   - Regenerate Rust-derived TypeScript declarations, JSON Schema, package
     runtime exports, and conformance fixtures. Document the direct-root and
     descendant forms as distinct versioned semantics.

2. **Expand the canonical producer and strict materializers.**
   - Keep the existing `contract.sessions` lifecycle controls and direct-root
     current-row Button oracle.
   - Change/add one multi-control current-row template whose root binds
     `@/session_uuid` and whose nested Buttons use at least `spawn`, `rename`,
     and `remove` keys. Each action payload must retain the selected row id and
     literal operation so proof can distinguish row and control.
   - Extend both authoritative Rust and generated Node reference
     materializers to recursively realize every descendant through the
     contract helper, in producer row and authored child order. Preserve
     strict rejection of malformed/extra canonical fixture shapes, unresolved
     values, blank root ids/keys, duplicate keys, and final collisions.
   - Publish stage expectations for multiple current rows, lifecycle changes,
     removal, and reconnect. Each stage records the exact root and descendant
     ids plus action payloads, rather than only aggregate counts.
   - Use adversarial row ids and keys containing colons, decimal-looking text,
     delimiter-like substrings, accented characters, CJK, and emoji to prove
     that distinct `(row, key)` pairs never collide across Rust and Node.
   - Edit the repo-root fixture as authority, synchronize the crate mirror, and
     generate the npm mirror. Update every shipped fixture/package README that
     describes revision 25's direct-root-only semantics.

3. **Prove Hub admission and the real producer path.**
   - Exercise generic contract validation and Hub-specific render/replacement
     admission. Accept the new form only below a bound item root; reject it at
     the surface root, direct item root, static child, literal/absent-root
     template, and empty template. Continue rejecting descendant full `$bind`.
   - Render the canonical package through registry admission, supervisor, Lua
     plugin worker, `PluginSurfaceRender`, Hub validation, client API, and
     daemon response. Structural fixture equality alone is insufficient.
   - Apply public session entity frames in the reference materializer and
     assert the exact realized identities. Dispatch representative row/control
     actions through real `PluginSurfaceAction` admission and the Lua handler;
     assert request/result `node_id` and payload identify the intended row and
     control exactly.
   - Extend the conformance report and daemon lifecycle assertions so the
     production entry point is what proves the new contract.

4. **Advance normal immutable artifacts as a cold contract update.**
   - The expected UI-contract coordinate is `0.3.0`: the public authored-id
     union gains a variant and every strict consumer must understand it. The
     expected Hub test-support coordinate is `0.1.19`, conformance revision is
     `26`, and daemon protocol remains `4`. These values are provisional until
     implementation rebases and repeats registry/revision collision checks.
   - Add no feature constant or protocol bump because daemon framing and
     request/response semantics do not change. Advance the conformance revision
     because published fixture content and renderer requirements do.
   - Regenerate all package metadata, checksums, fixtures, declarations,
     schemas, support matrices if implicated, READMEs, crate manifests, and
     lockfiles from authoritative sources.
   - Pack UI contract first, install that exact tarball into Hub test support,
     then pack Hub test support. Smoke both tarballs in clean external Rust and
     Node/TypeScript consumers. If npm credentials/2FA block publication,
     report the exact operator commands; do not create a path override or
     publication-only compatibility ticket.

5. **Hand the seam to separately routed renderer owners.**
   - Web ticket `ticket_1785602848_609148` depends on this Hub ticket via
     `dependency_1785602872_455279` and owns TypeScript entity materialization,
     React/DOM identity, browser action dispatch, reconnect, and real Web/Hub
     fixture proof.
   - TUI-kit ticket `ticket_1785602855_922302` depends on this Hub ticket via
     `dependency_1785602875_262989` and owns the exact contract repin plus any
     compile-only adaptation required to keep renderer state literal-only.
   - TUI ticket `ticket_1785602865_181673` depends on this Hub ticket and the
     kit ticket via `dependency_1785602878_170317` and
     `dependency_1785602881_268025`. It owns Rust entity materialization,
     focus/hit-map/InputRouter behavior, keyboard/mouse dispatch, reconnect,
     and real producer proof.
   - Hub implementation does not edit those repositories or claim their user
     paths shipped. The final project integration ticket consumes their merged
     evidence.

## Non-scope

- No edits to botster-web, botster-tui, botster-tui-kit, botster-workspaces,
  botster-core, or Project Pipelines from this worktree.
- No entity fields containing presentation-specific complete control ids.
- No descendant full-id `$bind`, string interpolation, delimiter joining,
  hashing, row-index ids, client-local prefix choice, collision repair,
  compatibility alias, or dual old/new descendant grammar.
- No widening of action request/result `node_id`, direct-root rewriting,
  alternate root-binding syntax, generalized expression language, typed
  BindList-template cleanup, entity query redesign, new primitive, or broad
  UiNode refactor.
- No product-specific Spawn/rename/remove policy in Hub. Those labels are
  representative literal fixture control keys only.
- No daemon protocol bump, new feature constant, subscription change, entity
  family, data-plane change, or durable Hub-state migration.

## Repository ownership boundaries and cross-repository dependencies

- **`botster-ui-contract` in this repository** owns the authored wire grammar,
  contextual validation, versioned encoding, Rust/JavaScript realization
  helpers, schema, TypeScript declarations, and contract fixtures. This is a
  renderer-neutral shared contract, not Hub runtime policy.
- **Hub runtime** owns package/surface admission and the exact production
  plugin-worker proof. It validates authored trees but has no entity-backed
  renderer store and therefore does not realize arbitrary live client trees.
- **`botster-hub-test-support` in this repository** owns the canonical producer
  fixture, strict Rust/Node reference materializers, generated package mirror,
  public conformance report, and downstream-consumable scenario.
- **`botster-hub-client` in this repository** owns conformance compatibility
  metadata. Only its fixture revision should change unless implementation
  reveals an actual daemon wire change.
- **botster-core** remains the lockfile-pinned policy-free runtime. No Core API
  change is expected. If the Hub cannot carry the shared contract through its
  current runtime seam, stop and route a Core prerequisite against
  `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`; do not fork a private contract.
- **botster-web** and **botster-tui** are peer renderer consumers and must
  converge on the same producer fixture and exact realized strings. Their
  registered tickets are downstream of this producer, not hidden work in the
  Hub run.
- **botster-tui-kit** owns reusable literal-identity mechanics. It must share
  the TUI's exact contract source but must not acquire entity materialization.
- **botster-workspaces** may later author real multi-control rows, but this
  ticket does not change its product surface. The conformance package proves
  the generic grammar before product adoption.

## Assumptions and unknowns

- The new wire tag is exactly `bind_list_descendant_id`, with a single `key`
  field and no extension fields. Plan Review should reject alternate spellings
  unless the committed plan and generated assets are updated together.
- The canonical prefix is exactly `botster-ui-descendant-v1:`. Its `v1` is the
  encoding version, not a compatibility path; a future encoding would require
  a new contract decision and cold migration.
- Decimal byte lengths and exact UTF-8 bytes make pair encoding injective.
  Renderers need only generate and compare the value; no decoder is required
  by this ticket.
- The nearest item-root `$bind` supplies row identity. Nested BindLists reset
  that context. The outer row is not implicitly composed into an inner row id;
  final render-scoped collision checks remain authoritative.
- Duplicate descendant keys are rejected across the entire authored template,
  even mutually exclusive branches, per the human answer. This is an explicit
  ticket-specific constraint alongside the general vault rule that final
  realized ids are checked only among coexisting nodes.
- Existing literal descendant ids remain accepted for wire compatibility, but
  the canonical multi-row fixture uses only the new form for identity-bearing
  controls and demonstrates collision rejection for repeated literals.
- UI contract `0.3.0`, Hub test support `0.1.19`, and fixture revision `26` are
  expected, not reserved. Mainline and npm registry state at implementation
  time are authoritative.
- Publication may require operator credentials or 2FA. All local, packed, and
  clean-consumer evidence remains mandatory before handing off an operator
  command.
- No ticket requirement is waived. If implementation needs normalization,
  parsing, a second identity source, or a different ownership boundary, stop
  and ask a human rather than broadening this grammar.

## Affected surfaces and files

- Plan: `docs/plans/define-bind-list-descendant-bound-identity.md`.
- Rust contract and tests: `crates/botster-ui-contract/src/lib.rs`,
  `src/assets.rs`, `tests/ui_contract_test.rs`,
  `tests/generated_assets_test.rs`, examples/generators implicated by runtime
  JS export generation, and `Cargo.toml`.
- Generated/published UI package: `packages/ui-contract/index.js`,
  `index.d.ts`, `schema.json`, `conformance-fixtures.json`, `test.mjs`,
  `README.md`, and `package.json`.
- Canonical producer source and synchronized mirrors:
  `fixtures/plugins/plugin-contract-matrix/{plugin.lua,README.md,botster-package.json}`,
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`, and
  `packages/hub-test-support/fixtures/plugin-contract-matrix/**`.
- Hub test support and generated npm assets:
  `crates/botster-hub-test-support/src/lib.rs`, relevant generator examples,
  `packages/hub-test-support/index.js`, `index.d.ts`, `test.mjs`, `README.md`,
  `session-plugin-binding-conformance-fixture.json`, `metadata.json`,
  `package.json`, and any source-derived support matrix/checksum files changed
  by regeneration.
- Hub admission/runtime proof: `src/runtime.rs`, the existing client API path
  if report wiring requires it, `tests/hub_client_api_test.rs`, and
  `tests/hub_daemon_lifecycle_test.rs`.
- Compatibility/release surfaces: `crates/botster-hub-client/src/lib.rs`,
  affected generated metadata/docs, crate manifests, `Cargo.lock`, root README
  identity text, and `script/publish-npm-packages` only if its existing
  two-package cold-release flow cannot carry the new runtime export.

This is an expected touch map, not permission for blanket rewrites. Every
changed line must trace to the descendant grammar, canonical materialization,
producer fixture, generated artifact consistency, stale documentation, or
required runtime/release proof.

## Risks and controls

- **Delimiter or Unicode collision:** use UTF-8 byte lengths, one fixed prefix,
  cross-language golden vectors, and adversarial pairwise non-collision tests.
- **JavaScript counts UTF-16 units:** make the npm helper use UTF-8 byte length
  (`TextEncoder`/equivalent), and compare exact Rust/Node outputs for multibyte
  values.
- **Direct-root 0.2.0 semantics silently widen:** retain a separate descendant
  variant and contextual tests; descendant `$bind` remains a hard error.
- **Authored keys become ambiguous:** reject blank and duplicate keys across the
  whole template before materialization.
- **Unresolved identities enter renderer state:** keep `as_literal()` behavior
  literal-only and require materialization before any hit/focus/action path.
- **Generated ids collide with literal/root ids:** retain final realized-tree
  uniqueness checks and negative controls; never rewrite either side.
- **Fixture exists but production is unwired:** require real package registry,
  plugin worker, Hub admission, daemon/client API action, and report evidence.
- **Source/generated fixture drift:** edit only source/Rust generators, run
  check-mode synchronization, and verify byte equality/checksums and packed
  contents.
- **Immutable version/revision collision:** recheck main and npm before version
  assignment and assert installed content, not metadata alone.
- **Downstream duplicate contract sources:** route the TUI-kit repin first and
  require TUI's dependency graph to resolve one `botster-ui-contract` source.
- **Consumer-local implementations drift:** export canonical Rust and npm
  helpers; downstream tickets must call them and include an ablation/negative
  proof that local synthesis is absent.
- **Over-strict key uniqueness conflicts with conditional render semantics:**
  record the human-selected authored-key rule explicitly while preserving
  render-scoped collision checks for final literal ids.

## Acceptance checks and tests

1. **Contract red/green and contextual negatives**
   - Show a checked descendant fixture fails against 0.2.0 and succeeds only
     after the new variant/validator lands.
   - Round-trip literal, root `$bind`, and descendant-key ids. Assert action
     request/result `node_id` remains string-only in Rust, schema, and TS.
   - Reject misplaced descendant forms, descendant `$bind`, absent/literal
     item-root context, empty-template use, blank keys, unknown fields,
     duplicate keys across children/slots/conditional branches, and malformed
     tag/key types.
   - Prove nested BindLists reset to their nearest root context.
   - Run:

     ```sh
     ./test.sh -p botster-ui-contract
     npm --prefix packages/ui-contract run check
     npm --prefix packages/ui-contract test
     ```

2. **Encoding parity and strict materialization**
   - Golden vectors assert exact output strings and UTF-8 byte counts in Rust
     and Node for ASCII, colons/digits/prefix-like content, whitespace,
     accented text, CJK, and emoji.
   - Pairwise vectors prove different `(row, key)` pairs never share output.
   - Multiple rows times `spawn`/`rename`/`remove` produce exact distinct ids in
     producer/child order, with matching row/control payloads at every fixture
     stage.
   - Both materializers reject duplicate keys, missing roots/rows, non-string
     or blank row ids, unresolved values, malformed/extra fixture controls,
     repeated literal descendants, and collisions with root/static nodes.
   - Run:

     ```sh
     ./test.sh -p botster-hub-test-support
     npm --prefix packages/hub-test-support run check
     npm --prefix packages/hub-test-support test
     ```

3. **Actual Hub producer/admission path**
   - Build the lockfile-pinned worker, record Hub SHA and locked Core SHA, and
     verify both binary realpaths belong to the fresh target directory.
   - Run the focused client/runtime and exact isolated-Hub conformance paths:

     ```sh
     cargo build --locked -p botster-core --bin botster-session-worker
     ./test.sh --test hub_client_api_test
     ./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
     ```

   - Require the real Lua worker body to match the canonical scenario, Hub to
     admit the authored descendant forms, the reference materializer to emit
     the exact ids, and representative row/control requests to pass through
     `PluginSurfaceAction` with exact `node_id` and payload echoed by the Lua
     result. Static JSON inspection or direct private dispatch is not proof.

4. **Repository gates**

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   ./test.sh
   git diff --check
   ```

   New regression tests must be shown red with the enforcement/helper change
   narrowly reverted or ablated, then green restored.

5. **Publication and clean consumers**
   - Repeat npm and revision preflight, commit all generated assets, and run
     `script/publish-npm-packages --dry-run` from a clean tree.
   - Install the exact UI tarball into Hub test support, then install both
     tarballs into clean temporary consumers. Assert versions, integrity,
     fixture revision/content, schema/declaration/runtime exports, exact golden
     ids, and strict TypeScript acceptance/rejection cases.
   - Publish UI contract before Hub test support. If credentials/2FA block the
     publish, attach exact verified operator commands and coordinates; after
     publication, repeat the external install proof from the registry.

6. **Required downstream proof**
   - Web ticket `ticket_1785602848_609148`: consume only published/merged
     artifacts, call the canonical npm helper, render multiple rows/controls,
     and click/keyboard-activate an exact non-first control. Assert React/DOM
     identity, structured action-result evidence, reconnect, Unicode vectors,
     duplicate diagnostics, and the real Hub transport path.
   - TUI-kit ticket `ticket_1785602855_922302`: repin the exact Hub revision,
     keep unresolved variants out of renderer state, run kit format/test/clippy
     and conformance suites, and prove one contract source.
   - TUI ticket `ticket_1785602865_181673`: call the canonical Rust helper,
     render multiple rows/controls through the production frame/hit map, Tab to
     a non-first control, dispatch keyboard and mouse input, reconcile focus
     after removal, reconnect, assert Unicode vectors/duplicate diagnostics,
     and run the published real producer scenario.
   - Cross-client success requires Web and TUI to produce the same exact id for
     every shared golden vector. Hub completion proves the producer seam; it
     does not substitute for these separately owned runtime paths.

## Vault gaps worth capturing

- After implementation and downstream proof, update
  [[ui contract row ids can bind before template expansion]] to distinguish
  the unchanged direct-root `$bind` from the new keyed descendant grammar.
- Capture the durable encoding rule: composed public identity uses a
  contract-owned versioned UTF-8 byte-length prefix, never delimiter joining or
  client-local synthesis.
- Reconcile [[post expansion identity uniqueness is scoped to one render not one tree]]
  with the stricter authored descendant-key uniqueness chosen here: control
  keys are template-global, while final literal-id collision checks remain
  render-scoped.
- No vault note should claim the new grammar is shipped until Rust/Node parity,
  exact Hub admission, published package consumption, and both renderer tickets
  are verified. The plan itself is proposal evidence, not implementation
  evidence.
