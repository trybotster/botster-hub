# Accept bound sentinels for required bindable UI fields

## Target and repository routing

- Ticket: `ticket_1785617154_342333`, “Hub UI contract: accept bound
  sentinels for required bindable fields.”
- Target: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through the Hub spawn-target
  registry to `trybotster/botster-hub` (`botster-hub`). The run worktree remote
  is `https://github.com/trybotster/botster-hub.git`.
- Planning base: `1955c9e0713281093f609d09f6597a1dcfaf07d3`, equal to
  `origin/main` when planning began. The worktree was clean before this plan
  artifact was added.
- Repository charter: [[botster-hub-playbook]]. The Hub repository owns this
  renderer-neutral sibling contract package, Hub package/surface admission,
  generated npm assets, and Hub test support; it must not acquire renderer
  materialization or TUI input policy.

## Context loaded

Role and repository guidance was loaded in the required order:

1. [[planner-playbook]]
2. [[botster-planner-playbook]]
3. [[botster-hub-playbook]]
4. Task-surface guidance: [[botster-package-reviewer-playbook]] and
   [[botster-package-verifier-playbook]]

The Botster maps and targeted atomic notes used were:

- [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]
- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[plugin surface handlers must validate against hub locked uinode contract]]
- [[plugin surfaces request model state through ui bindings not hub subscribe]]
- [[plugin dynamic ui lists bind to plugin-owned entities]]
- [[ui contract row ids can bind before template expansion]]
- [[ui bind list typed templates are narrower than the runtime wire grammar]]
- [[hub supervision admission changes require exact live hub launch proof]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[hub test support npm releases need external consumer smoke]]
- [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]
- [[conformance fixture revisions must be unique per published content]]

The remaining Hub-charter must-load notes were also checked; they impose no
additional lifecycle, data-plane, durable-state, WebRTC, or plugin-worker
resource changes on this validation-only ticket. [[project-pipelines-playbook]]
was not loaded because neither Project Pipelines package/plugin source nor
workflow policy is in implementation scope. Project Pipelines checklist tools
are still being used for this run's workflow evidence.

Repository evidence inspected:

- `README.md`, `docs/client-protocol.md`, the two package READMEs, current
  `docs/plans` prior art, root `test.sh`, package sync/check scripts, Cargo and
  npm manifests, and the publish script.
- `crates/botster-ui-contract/src/lib.rs`, asset generation, generated-asset
  and semantic contract tests, and checked npm outputs.
- `src/runtime.rs` production render/action-result admission and its binding
  family tests.
- the source, Rust-owned, and npm-mirrored plugin contract matrix; session
  binding fixtures and Rust/Node materializers; Hub-client compatibility
  metadata.
- Originating consumer run `run_1785614950_505375`: its TUI implementation
  reaches unconditional authored `surface.body.validate()` and fails before
  materialization with `Button missing required label` for
  `label: {"$bind":"@/lifecycle_class"}`. That run has a registered blocking
  dependency on this ticket and intentionally contains no client workaround.
- Plan baseline: `./test.sh -p botster-ui-contract` passed 78 tests (3 generated
  asset tests and 75 semantic tests), and
  `./test.sh -p botster-hub-test-support` passed 42 unit tests plus 3 doctests.
- Registry preflight on 2026-08-01: public npm latest is
  `@trybotster/ui-contract@0.2.0` and
  `@trybotster/hub-test-support@0.1.18`; this source tree already prepares the
  unpublished `0.3.0` and `0.1.19` coordinates.

## Human rulings and assumptions

Two blocking Project Pipelines questions removed the field-classification
ambiguity:

- `question_1785617615_993233`: generic Rust recognition of a `$bind` object
  does not by itself declare a field bindable.
- `question_1785617674_572099`: preserve the union of fields explicitly typed
  string-or-bind and fields already proven by production/conformance behavior.
  In particular, retain the shipped `Text.text` binding; do not narrow it to a
  literal.

The authoritative required-bindable matrix has two semantics classes:

- **Class A — nonblank string or valid sentinel:** `Button.label`,
  `IconButton.label`, `MenuItem.label`, `Form.submit_label`, `Iframe.src`, and
  `Iframe.title`. Form/Iframe already implement these semantics; the defect is
  the one literal-only `validate_required_label` branch shared by the three
  action-node kinds.
- **Class B — existing presence-only literal semantics plus valid sentinel:**
  `Text.text`, preserved from the real Hub/session-binding producer and current
  contract tests. `Text.text` has no literal value validator today, so this
  ticket must continue accepting existing literals such as `""`, numbers, and
  `null` while protecting its shipped binding behavior. Tightening those
  literals would be a separate cold contract change with a downstream producer
  audit and human ruling.

Before editing, Implement must run one final tree-wide inventory for production
or conformance evidence of another required bindable field. A field joins the
positive matrix only if that audit finds authoritative existing evidence; it
must not join merely because `validate_prop_value` happens to recognize a
`$bind` object. Representative required non-bindable fields include
`Form.action`, input `name`, `SelectOption.value`, `Table.columns`, and Custom
owner/reason fields. These remain literal/structural and must reject a sentinel.

Assumptions:

- `UiNode::validate()` remains the compatible authored-tree entry point used
  by Hub and the blocked TUI consumer. The implementation may add an explicitly
  named authored alias for clarity, but must not make downstream callers opt
  into the bug fix.
- Realized/post-materialization validation needs an explicit strict entry
  point because the same sentinel is valid before materialization and invalid
  afterward. The narrow expected API is `UiNode::validate_realized()` plus a
  free-function counterpart, implemented by one internal phase/context rather
  than a second validator.
- That realized entry point is a consumer-facing shared-contract API. Hub has
  no UiNode materializer by design: it validates authored trees and transports
  them. In-repository proof therefore comes from contract tests plus the strict
  Rust/Node Hub test-support materializers; the production caller is the
  blocked TUI run (and later Web) after repin.
- JSON Schema can describe literal-or-bind structure but cannot prove
  materialization phase. Rust remains authoritative; TypeScript and docs must
  make the phase boundary explicit.
- This changes shipped/prepared contract assets, so the next coordinates are
  `botster-ui-contract` / `@trybotster/ui-contract` `0.3.1` and
  `@trybotster/hub-test-support` `0.1.20`, subject to an immediate registry and
  mainline collision check during Implement. Do not mutate the already-prepared
  `0.3.0`/`0.1.19` identities in place: their tarballs and revision-26 bytes
  may already have been packed or consumed locally by the blocked TUI run and
  other in-flight consumers, so reusing those identities with different bytes
  would recreate the stale-artifact/revision-collision hazard captured by
  [[conformance fixture revisions must be unique per published content]].
- The fixture-content change advances Hub conformance revision from 26 to 27,
  again subject to checking current main and every published artifact before
  use. It does not change daemon framing, so protocol version remains 4 and no
  feature flag is added.

Unknowns to resolve from the diff, without broadening scope:

- The smallest implementation should change `validate_required_label` to
  accept a shape-checked sentinel or nonblank literal, leaving the already-
  correct Form/Iframe validators and presence-only Text enforcement untouched.
  Introduce private shared field metadata only if generated TypeScript/Schema
  parity cannot be expressed without it, and record that necessity in the
  Implement report; do not refactor requiredness speculatively.
- Whether the current strict Rust/Node session materializers can expose the
  realized bound label using their existing report shapes. Prefer adding the
  smallest assertion/report field needed for parity; do not redesign their
  APIs.

## Scope

1. **Fix the narrow required-label branch and add strict realized validation.**
   - First make the surgical fix: teach `validate_required_label` to accept a
     shape-checked `UiBind` or a nonblank literal for Button, IconButton, and
     MenuItem. Leave Form/Iframe routing through
     `validate_nonblank_string_or_bind_prop` and Text's required-presence-only
     literal semantics unchanged.
   - For Class A, missing keys, `null`, empty/whitespace literals, wrong literal
     types, empty/invalid paths, non-string `$bind`, and sentinel objects with
     extra keys remain errors with field-specific diagnostics. For Class B,
     only required presence and valid-sentinel structure are in scope; lock the
     current permissive literal behavior with an explicit regression.
   - Preserve existing optional binding behavior and action-payload binding
     behavior. Do not use this ticket to infer new bindable required fields.
   - Add strict realized validation over the same recursive validator. It
     rejects every unresolved sentinel in a realized node while applying each
     field's existing literal rules: Class A remains nonblank string, while
     Class B remains presence-only. Keep authored
     identity context and descendant-key validation intact.
   - Ensure capability validation still follows semantic validation and does
     not accidentally treat authored sentinels as final renderer values.

2. **Align generated TypeScript, JSON Schema, runtime metadata, fixtures, and
   docs.**
   - Introduce a generated `UiBindableString = string | UiBind` (or equivalent
     narrowly named type) for Class A. Give `Text.text` an explicit authored
     binding declaration while retaining its existing JSON-literal breadth;
     do not falsely type it as nonblank string. Keep non-bindable required
     fields literal/structural in TypeScript.
   - Generate matching JSON Schema branches for those fields. Retain nonblank
     literal constraints only for Class A and preserve Text's current literal
     schema while admitting a valid sentinel. Add Schema negatives for
     malformed sentinel shapes and required non-bindable sentinels.
   - Add generated conformance cases for authored bound required values,
     materialized literals, and unresolved-realized failure. Update package
     runtime version metadata and package tests to assert the new declarations,
     schema branches, and fixtures.
   - Document authored versus realized validation and the exact required
     bindable field inventory in `packages/ui-contract/README.md`, the Hub
     README/client protocol where the runtime boundary is described, and the
     Hub test-support README/release history.

3. **Exercise the actual Hub admission and transport path.**
   - Make `validate_plugin_surface_node` and accepted replacement validation
     visibly use the authored contract boundary while preserving the Hub-only
     `/session` binding-family admission pass.
   - Add focused runtime tests around the changed label branch plus parity
     coverage for the already-correct Form/Iframe paths and existing Text
     binding. Class A gets malformed/missing/empty literal negatives; Text gets
     a deliberate permissive-literal regression. Add a representative required
     non-bindable sentinel negative. The load-bearing production regression is
     the exact bound Button label that currently fails.
   - Change one canonical `contract.sessions` Button label to the existing
     item-relative `@/lifecycle_class` binding while retaining the existing
     `Text.text` binding. Carry the source fixture through the Rust-owned and
     npm mirrors; extend strict Rust/Node reference materialization only enough
     to prove the label resolves to the row's literal lifecycle class and no
     unresolved required value reaches realized output.
   - Through a real isolated Hub and plugin worker, prove the authored tree is
     admitted and transported as a typed `plugin_surface_render` response.
     Then prove the reference materializer emits a literal label and strict
     realized validation passes. Add negative admission for malformed bind and
     negative realized proof for an unresolved sentinel.

4. **Prepare coordinated release artifacts without publishing.**
   - Bump Rust/npm UI contract to `0.3.1`, Hub test support to `0.1.20`, the
     support package's exact UI dependency to `0.3.1`, metadata/checksums, and
     conformance revision to 27 after collision checks.
   - Regenerate all checked assets through existing generators. Never hand-edit
     generated protocol, fixture, metadata, schema, or declarations when the
     repository names a generator.
   - Pack both packages, install their tarballs together in a clean temporary
     non-Hub consumer, and prove versions, dependency resolution, schema/types,
     bound-label fixture content, asset verification, and materialization.
   - Publication is a manual operator action only after merge and final Verify.
     Record the exact commands/coordinates, but do not publish from an agent
     implementation step.

## Non-scope

- No botster-tui, botster-tui-kit, botster-web, botster-core, Workspaces, or
  Project Pipelines source edits in this run.
- No client-side validator, workaround, validation skip, literal replacement
  of the bound label, compatibility grammar, dual validator implementation, or
  old/new version path.
- No declaration of new bindable fields based solely on generic JSON shape;
  no expression language, coercion, fallback, interpolation, or broad UiNode
  type cleanup.
- No renderer focus, hit-map, action dispatch, entity hydration, package
  workflow, daemon protocol, feature, data-plane, lifecycle, durable-state, or
  plugin-worker resource changes.
- No opportunistic cleanup of older generated/docs inconsistencies except
  lines directly required to make these contract versions and fixtures agree.
- No npm publication or downstream consumer repin before the Hub change is
  merged and verified.

## Ownership boundaries and cross-repository dependencies

- **In-repository `botster-ui-contract`:** owns authored/realized grammar,
  field-aware validation, schema, TypeScript, fixtures, and package version.
- **Hub runtime:** owns installed surface admission and binding-family policy.
  The actual user path is
  `plugin worker -> HubRuntime::render_plugin_surface -> UiNode authored
  validation -> Hub binding-family admission -> HubClientApi/daemon typed
  response`.
- **In-repository Hub test support:** owns source-derived producer fixtures,
  strict Rust/Node reference materializers, isolated-Hub transport proof,
  mirrored assets, compatibility metadata, and the downstream-consumable npm
  oracle.
- **botster-core:** remains the lockfile-pinned policy-free runtime. No Core API
  change is expected. If the current plugin-worker seam cannot carry the fixed
  contract, stop and register a Core-targeted prerequisite rather than copying
  the contract.
- **botster-tui:** originating run `run_1785614950_505375` already registers
  this Hub ticket as a blocking dependency. Once this merges, that run owns the
  exact repin, post-materialization TUI validation, focus/hit/input proof, and
  removal/reconnect acceptance. Hub must not edit its partial worktree.
- **botster-web:** owns its eventual exact normal-registry repin and renderer
  proof. The clean tarball consumer smoke here proves the distributable seam,
  not shipped browser behavior.
- **botster-tui-kit:** owns reusable literal renderer mechanics; no change is
  expected because this ticket does not alter authored identity types.

There is no new cross-repository prerequisite for this Hub run. The existing
downstream TUI dependency is correctly registered in the consuming TUI ticket,
so this plan does not create a reverse dependency or silently broaden scope.

## Affected surfaces and files

Expected direct edits:

- `crates/botster-ui-contract/src/lib.rs`
- `crates/botster-ui-contract/src/assets.rs`
- `crates/botster-ui-contract/tests/ui_contract_test.rs`
- `crates/botster-ui-contract/tests/generated_assets_test.rs`
- `crates/botster-ui-contract/Cargo.toml` and `Cargo.lock`
- generated/checksummed `packages/ui-contract/{package.json,index.js,index.d.ts,schema.json,conformance-fixtures.json,README.md}`
- `src/runtime.rs`
- canonical and mirrored
  `fixtures/plugins/plugin-contract-matrix/**`,
  `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`, and
  `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
- `crates/botster-hub-test-support/src/lib.rs`
- `packages/hub-test-support/{package.json,index.js,index.d.ts,metadata.json,session-plugin-binding-conformance-fixture.json,test.mjs,README.md}` and only
  generator-owned copies/checksums implicated by synchronization
- `crates/botster-hub-client/src/lib.rs` for conformance revision 27; generated
  daemon protocol only if the existing generator changes it because of that
  authoritative metadata
- `README.md` and `docs/client-protocol.md`

`script/publish-npm-packages` and sync scripts should remain unchanged unless
the existing generic release flow cannot pack the two coordinated versions;
do not add ticket-specific release logic.

## Risks and mitigations

- **Accidental broadening or narrowing:** generic `$bind` recognition currently
  obscures the intended field contract. Pin Class A's six nonblank-string
  fields separately from Class B's presence-only `Text.text`, add
  representative non-bindable negatives, and preserve the existing Text
  producer and permissive literal regression before changing validation.
- **Authored/realized ambiguity:** changing `validate()` to strict realized
  semantics would recreate the TUI failure; leaving no strict phase would let
  unresolved values pass after materialization. Preserve `validate()` as
  authored and add one explicit realized entry point over shared internals.
- **Bespoke-label regression elsewhere:** Button, IconButton, and MenuItem share
  required label logic. Matrix tests must cover all three, not just Button.
- **Malformed sentinel mistaken for a literal object:** require exactly one
  string `$bind` field and validate `/` or `@/` path syntax before accepting the
  authored branch.
- **Schema/TypeScript drift:** generated assets currently type Form/Dialog/
  Button required fields differently from Rust. Generator tests must assert
  every matrix member and negative, rather than token existence alone.
- **Fixture proof stops at code existence:** require the real plugin-worker and
  Hub transport response plus literal reference materialization; a static JSON
  assertion or direct callback is insufficient.
- **Conformance revision collision:** re-read current main and all published
  package metadata immediately before choosing 27; assert defining fixture
  content as well as the number.
- **Same-version cache/stale artifact:** use new `0.3.1`/`0.1.20` identities,
  fresh build targets for live proof, tarball installs in a new temp project,
  and exact metadata/dependency assertions. Prepared `0.3.0`/`0.1.19` and
  revision-26 bytes may already exist in local consumer caches even though the
  registry latest values are older.
- **Publication before merge:** keep publication manual and final; pack/install
  locally during Implement and repeat registry install verification only after
  the operator publishes.
- **Hub/Core provenance confusion:** final live proof records the merged Hub SHA
  and the separate lockfile-pinned Core SHA/worker realpath.

## Acceptance checks and tests

1. **Red/green contract matrix**
   - Add a focused pre-fix regression using the exact authored Button label
     sentinel from the TUI failure and record that it fails as
     `Button missing required label`; after the fix it passes.
   - Rust authored validation accepts all seven classified fields with valid
     absolute or row-relative sentinels. Class A accepts nonblank string
     literals and rejects missing, `null`, empty/whitespace, and wrong-type
     literals. Class B preserves required presence but deliberately continues
     accepting `text: ""`, `text: 42`, and `text: null`.
   - Empty/relative-invalid bind paths, non-string `$bind`, and extra sentinel
     keys fail clearly. Required non-bindable fields reject a sentinel. The
     existing bound `Text.text` producer/tests continue to pass.
   - Strict realized validation accepts the corresponding materialized literal
     trees and rejects an otherwise-valid tree with any unresolved sentinel.
   - `UiNode::validate()`, the authored free function, action-result replacement
     validation, and capability entry points retain consistent authored
     behavior; the strict realized functions are recursively consistent.
   - Run:

     ```sh
     ./test.sh -p botster-ui-contract
     npm --prefix packages/ui-contract run check
     npm --prefix packages/ui-contract test
     ```

2. **Generated and package parity**
   - Generated TypeScript exposes the bindable-string union on Class A and an
     explicit bindable-but-literal-permissive Text value on Class B;
     `UiAction`, input `name`, `SelectOption.value`, request and result
     `node_id` stay non-bindable.
   - JSON Schema accepts valid matrix sentinels, rejects malformed and
     representative non-bindable sentinels, enforces missing/empty literal
     negatives for Class A, and preserves Text literal permissiveness.
   - Rust fixtures, source plugin fixture, crate copy, npm copy, metadata,
     checksums, and both hand-authored Node materializers remain current and
     agree on the bound label and its realized literal.
   - Run:

     ```sh
     ./test.sh -p botster-hub-test-support
     npm --prefix packages/hub-test-support run check
     npm --prefix packages/hub-test-support test
     ```

3. **Hub admission, transport, and real runtime**
   - Focused `src/runtime.rs` tests prove authored render and accepted
     replacement admission for bound required fields, malformed rejection, and
     unchanged `/session` family policy.
   - Build the locked worker and run the real contract matrix through the Hub:

     ```sh
     cargo build --locked -p botster-core --bin botster-session-worker
     ./test.sh --test hub_client_api_test
     ./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
     ```

   - Assert the typed daemon response contains the bound Button label from the
     real Lua worker, the strict reference materializer resolves it to the
     matching row lifecycle string, realized validation passes, and malformed
     or unresolved-realized controls fail through their intended boundaries.
   - The harness owns an isolated data directory and must explicitly shut down
     any created session before stopping the Hub. Final Verify uses fresh target
     realpaths and records the merged Hub SHA separately from the lockfile-pinned
     Core worker SHA.

4. **Workspace quality gates**

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   ./test.sh
   ```

   Any pre-existing failure needs an exact unrelated-failure proof; it is not a
   blanket waiver for this contract change.

5. **Release artifact and downstream-shaped proof**
   - Immediately before versioning, query all public versions and confirm
     `0.3.1`, `0.1.20`, and revision 27 are unused and current relative to
     merged main/published bytes.
   - Run each package's existing `npm pack --dry-run --json`, then create actual
     tarballs. Install both into a new temporary consumer with no sibling Hub
     path. Assert:
     - UI `packageVersion === "0.3.1"`;
     - support `metadata.package_version === "0.1.20"`, revision 27, and exact
       UI dependency/metadata `0.3.1`;
     - `verifyPackageAssets()` passes;
     - installed declarations/schema encode the exact positive and negative
       field matrix;
     - the installed fixture materializes the bound Button label to a literal;
     - both installed `LICENSE` files exist.
   - Do not publish. After merge and manual operator publication, repeat the
     same clean-consumer smoke against registry coordinates.
   - Resume `run_1785614950_505375` only after it repins the merged Hub revision;
     its previously failing production TUI test must pass without a local
     validation skip or literal-label workaround. That separately routed run
     owns full renderer/focus/hit/input proof.

## Pipeline gates and artifacts

- Plan artifact: this file plus the structured `botster_stack_plan_gate`
  evidence.
- Implement report must state the final field matrix, both human rulings,
  versions/revision, changed files, generator commands, red/green regression,
  exact Hub/Core provenance available at that stage, live transport evidence,
  pack/install evidence, downstream status, deviations, and unverified items.
- Review must reject generic-bind broadening, Text narrowing, generated drift,
  a realized validator that is unexported, lacks strict reference-materializer
  proof, or is unreachable from the published contract surface, static-only
  admission evidence, or publication from the agent step. It must not require
  an in-Hub production materialization caller: Hub intentionally owns authored
  admission, while TUI/Web own realized consumption after repin.
- Verify must repeat fresh merged-binary and clean-registry/tarball evidence as
  applicable, then notify the blocked TUI run that its dependency is consumable.

## Vault gaps worth capturing

Implementation is likely to establish one durable contract rule not yet
captured atomically: **required UiNode bindability is explicit per field and
does not imply uniform literal semantics, while authored and realized
validation differ on unresolved sentinels**. If implementation confirms that shape, capture
it through the vault inbox/document/connect/verify pipeline and link it from
the Botster architecture map. Do not create a note merely restating this
ticket. If implementation only applies existing guidance without revealing a
reusable rule, record `capture_path: nil` and that reason in the vault
checklist.

No loaded convention conflicts with the plan. It follows the Hub/UI-contract
ownership split, preserves the existing binding producer, uses the real Hub
admission path, avoids client workarounds and compatibility code, keeps release
publication manual, and confines every planned line to the ticket intent or
generated parity required by that change.
