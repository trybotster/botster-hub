# Prove live presentation set/open through plugin surface actions

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785211693_467262`
- Run: `run_1785211706_123174`
- Planned base: `origin/main` at `d79403c`
- Assigned worktree: the Project Pipelines ticket worktree for this run.
- Repository charter: [[botster-hub-playbook]]
- Role guidance: [[planner-playbook]] and [[botster-planner-playbook]]
- Surface guidance: [[botster-package-reviewer-playbook]]
- Architecture maps: [[botster-architecture]], [[cli-patterns]], and
  [[spa-patterns]]
- Self context: [[identity]] and [[goals]]
- Hub ownership notes:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]], and
  [[webrtc bootstrap origin must be requested after the package server binds]]
- Package, UI, and conformance notes:
  [[botster first party client support matrices belong in hub test support]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[hub test support npm releases need external consumer smoke]],
  [[plugin surface actions route by explicit metadata]],
  [[conformance helpers must dispatch the action id read from the rendered node]],
  [[conformance oracles assert action result frames not toast text]],
  [[conformance fixture revisions must be unique per published content]],
  [[published fixture readmes are part of the shipped contract]],
  [[botster web form actions must preserve collected values into transport payloads]],
  [[botster plugin modal state belongs in client-local presentation state]], and
  [[closed dependency tickets signal merged source not a consumable release]]
- Workflow notes:
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[plan agents must author vault context as wikilinks not home paths]], and
  [[vault example paths are not repository placement conventions]]

[[project-pipelines-playbook]] was not loaded: this run uses Project Pipelines
workflow tools, but no Project Pipelines package/plugin path or workflow policy
is in the implementation scope.

## Current repository facts

- The UI-contract producer ticket is merged on the planned base. The standalone
  `botster-ui-contract` crate and published `@trybotster/ui-contract@0.1.0`
  already own typed `UiActionRequest`, `UiActionResult`,
  `UiPresentationOperation::{Set,Clear,Toggle}`, scoped
  `presentation_if` predicates, Dialog validation, replacements, and
  deterministic conformance fixtures.
- The daemon protocol already uses the canonical
  `PluginSurfaceAction { package_name, request: UiActionRequest }` request and a
  typed `UiActionResult` response. `HubRuntime::dispatch_plugin_surface_action`
  sends the full request through the real plugin worker, validates the typed
  result, and rejects request/surface/action/node identity mismatches.
- The source fixture is
  `fixtures/plugins/plugin-contract-matrix`. Its Rust crate mirror and npm
  package mirror are generated/checked from that source.
- The fixture surface already publishes a `Dialog` under a scoped
  `contract-dialog` presence predicate and a text node under an equality
  predicate for `selected-workspace == "workspace-alpha"`.
- The current live fixture action proves values, accepted/rejected/error
  states, replacement validation, identity validation, and a presentation
  `clear`, but no real action emits `set` or `toggle`. The live harness checks
  binding declarations and action-result JSON separately; it does not apply
  the returned presentation operations to browser-shaped scoped state or prove
  that the resulting tree exposes the dialog/equality-bound node.
- `run_plugin_contract_matrix_conformance` already installs/enables the copied
  packaged fixture, renders through the daemon, and dispatches through a real
  isolated Hub/plugin worker. The hub integration test is
  `daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`.
- Checked-in `@trybotster/hub-test-support` is already `0.1.12`, protocol
  version 4, conformance revision 19. Registry inspection during planning found
  published hub-test-support versions only through `0.1.11`; therefore
  `0.1.12` is the single staged, unpublished release that must absorb this
  producer proof. This ticket must not bump it again or publish a partial
  `0.1.12`.
- `packages/hub-test-support/test.mjs` still contains a pre-publication bridge
  that creates `node_modules/@trybotster/ui-contract` as a symlink to the
  repository sibling package before reading the UI-contract fixtures. Its own
  “until publication” premise has expired because
  `@trybotster/ui-contract@0.1.0` is published. This run must remove the bridge
  and its now-unused filesystem imports so the normal package test can resolve
  the declared registry dependency honestly.

## Scope

### 1. Make the packaged fixture produce live presentation effects

Change the canonical source fixture so action descriptors in the rendered tree
carry the operation metadata needed to drive one coherent interaction:

1. A rendered toolbar/button action opens the contract dialog. Its real plugin
   worker result is `accepted` and carries two typed `set` operations:
   `contract-dialog` receives a present/truthy value and
   `selected-workspace` receives `"workspace-alpha"`.
2. The rendered form action submits through the same canonical request envelope.
   Empty/invalid `values.message` returns `rejected` with the existing
   field/form error keyed to `contract-app-message`, and no accepted-only
   presentation or replacement effects.
3. A valid form submission returns `accepted`, preserves normalized form
   values and the owner-authored replacement tree, and clears
   `contract-dialog`.
4. Add a distinct rendered toolbar button for a `toggle` operation and keep the
   existing valid-submit `clear` result so all three typed presentation
   operations are exercised by real actions. The toggle action uses the same
   semantic handler with a rendered operation payload; the harness reads the
   button's id, node id, and payload before dispatch. Keep generic error,
   identity-mismatch, and malformed-replacement cases as deliberately
   harness-authored negative probes with no public rendered controls.

Use semantic action metadata read from rendered nodes; do not introduce
fixture-specific daemon branches or a second request shape. Keep the existing
`contract.action` handler with small operation payloads for the rendered open,
toggle, and form-submit controls. For that user-shaped live sequence, every
action id, node id, and payload must be read from the rendered action descriptor
rather than duplicated in the harness. Generic error, identity-mismatch, and
malformed-replacement probes remain harness-authored payloads because they are
intentional boundary attacks, not user-visible controls; they still reuse the
action identity first discovered from the rendered tree.

### 2. Extend the live conformance runner with browser-shaped state

Extend `run_plugin_contract_matrix_conformance` and its stable report with a
small conformance-only client model that is scoped by package plus surface. It
must consume the real validated snapshot and typed action results, not the
static UI-contract fixture:

1. Render `contract.app`, locate the open action, and build the canonical
   `UiActionRequest` from its rendered id/payload and emitting node id.
2. Dispatch through the isolated daemon and plugin worker.
3. Require an accepted typed result with both expected `set` operations, apply
   only accepted presentation operations to the scoped client-local map, and
   evaluate the snapshot's real `presentation_if` presence/equality bindings.
4. Record that `contract-dialog` and `contract-selected-workspace` are visible
   after the set result. This is the browser-shaped open-state proof.
5. Read the form action from the rendered form, submit invalid values, and
   prove rejection retains the original tree, open dialog state, selected
   workspace equality state, and actionable field/form errors.
6. Submit valid values, prove the worker received the values and returned the
   accepted normalized values/replacement, apply the clear operation, and
   prove the dialog is no longer visible.
7. Read the distinct toggle button metadata from the rendered toolbar,
   dispatch it, and verify deterministic state transitions without moving
   presentation storage or renderer policy into Hub production code.

The client model is test-support mechanics, not a new public rendering
framework. Keep it private unless a narrow public helper is necessary for
downstream conformance. The Rust report should expose stable live observations
that TUI/TUI-kit consumers can compare, including set keys/values, visibility
before and after effects, rejected-state retention, clear/toggle observations,
form-value round trip, replacement id, and exact action identity.

Web cannot consume that Rust report directly. Extend the source-derived
`FirstPartyClientSupportMatrix.plugin_surfaces` block and its published JSON
with the TypeScript-reachable expected facts proven by the live runner:
runner name, emitted presentation operation kinds, dialog presence key, selected
workspace equality key/value, and the two authored set key/value pairs. The
Rust stable-shape test must pin those fields, and `test.mjs` must assert them
from `first-party-client-support-matrix.json`. This gives Web a packaged
comparison basis while the real Rust runner remains the proof that the producer
actually emits and applies those facts.

### 3. Regenerate and document the normal test-support artifact

- Regenerate the Rust crate fixture mirror and npm fixture mirror from the
  canonical source; do not hand-edit generated copies.
- Run the normal hub-test-support asset sync so metadata checksums cover the
  changed fixture README, Lua bytes, and generated first-party support matrix.
- Keep package version `0.1.12`, protocol version 4, and conformance revision 19
  unless implementation changes a separately revisioned deterministic fixture.
  This ticket adds live producer/conformance evidence and packaged fixture
  bytes; it must not manufacture a compatibility envelope or a second
  `0.1.12` release.
- Update the source fixture README, generated fixture READMEs, npm package
  README, `docs/client-protocol.md`, and root README claims that enumerate the
  current matrix/version or live proof. The docs must distinguish the live
  Hub/plugin-worker/browser-shaped sequence from the complementary static
  `@trybotster/ui-contract` fixtures.
- Preserve the existing package API and dependency on the published
  `@trybotster/ui-contract@0.1.0`. Delete the expired test-only sibling symlink
  bridge and its unused imports, install the declared dependency from the
  registry for package tests, and do not add local, file, workspace, or sibling
  overrides.

## Non-scope

- No edits to `botster-web`, `botster-tui`, `botster-tui-kit`,
  `botster-core`, `botster-workspaces`, or `botster-project-pipelines`.
- No production browser renderer, React/Ionic component, TUI widget, focus
  manager, or client presentation-store implementation.
- No Hub-owned durable presentation state, renderer policy, workspace product
  vocabulary, or special handling for `contract.action`.
- No redesign of the standalone UI contract, daemon protocol, package manifest,
  plugin-worker ABI, or action-result state machine.
- No compatibility request envelope, split action fields, untyped action-result
  body, `props.open` exception, version suffix, or sibling checkout fallback.
- No unrelated fixture cleanup, historical plan/report rewrite, or speculative
  abstraction.
- No npm publication from this run. If credentials or 2FA are needed after
  merge, hand off the one exact operator command documented below.

## Repository ownership boundaries and cross-repository dependencies

- `botster-ui-contract` remains the authority for renderer-neutral types,
  validation, presentation operations/predicates, and static fixtures. This run
  consumes those types and does not alter their ownership.
- Hub owns package admission, explicit package/surface/action routing, plugin
  worker invocation, result validation, daemon projection, and the isolated
  live harness.
- `botster-hub-test-support` owns the downstream-shaped process harness,
  packaged contract fixture, browser-shaped conformance model, and stable
  report. It must continue depending only on public client/UI contracts rather
  than linking private Hub runtime internals.
- Web and TUI own actual presentation stores and rendering. Their existing
  adoption tickets consume this producer after merge/publication; they are
  downstream dependents, not scope for this run.
- There is no blocking cross-repository prerequisite: the required UI contract
  source is merged and `@trybotster/ui-contract@0.1.0` is published.
- If registry state changes and `@trybotster/hub-test-support@0.1.12` appears
  before this work merges, stop and ask the human. Publishing a different
  content meaning under an existing coordinate or silently choosing another
  version would violate the ticket.

## Assumptions and unknowns

- Assumption: “browser-shaped” means a conformance client that applies scoped
  accepted presentation operations and evaluates the delivered tree exactly as
  a renderer would; it does not authorize a fake browser product path or
  cross-repository Web implementation.
- Assumption: a present boolean value is sufficient to open the presence-bound
  dialog; the exact authored value should be asserted so clients do not infer
  an untyped open convention.
- Assumption: setting `selected-workspace` to `"workspace-alpha"` in the same
  accepted result is the smallest proof that equality binding is populated by
  the real producer.
- Assumption: rejected/error results are not applied to presentation or
  replacement state, consistent with `UiActionResult::validate`.
- Assumption: the existing `0.1.12` version bump belongs to the merged producer
  ticket and should remain unchanged here because the registry still stops at
  `0.1.11`.
- Assumption: the new toggle case is a distinct rendered toolbar button using
  the existing action handler plus an operation payload; this proves toggle
  through rendered metadata without exposing negative test controls.
- Assumption: the Rust report is the live comparison surface for Rust
  consumers, while the generated first-party support matrix publishes the
  corresponding TypeScript-reachable expected facts for Web.
- Assumption: removing the expired npm-test sibling bridge is necessary cleanup
  caused by this ticket's package-test and no-override acceptance boundary, not
  an adjacent package-manager refactor.
- Unknown: whether the stable report needs a nested interaction-sequence struct
  or additive flat fields. Prefer the smallest shape that remains readable and
  serializable for downstream comparison.

No human question is currently required because repository ownership, the
canonical action shape, the release coordinate, and the required live sequence
are all resolved by the ticket plus current mainline evidence. The implementer
must ask rather than improvise if the package coordinate becomes published or
the requested sequence cannot preserve accepted-only effect validation.

## Affected surfaces and files

- Canonical producer:
  - `fixtures/plugins/plugin-contract-matrix/plugin.lua`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
- Rust test-support and live proof:
  - `crates/botster-hub-test-support/src/lib.rs`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `tests/hub_lua_runtime_test.rs` only if a focused in-process worker
    regression is needed in addition to the isolated-daemon proof
- Generated/package mirrors:
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
  - `packages/hub-test-support/first-party-client-support-matrix.json`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/test.mjs`
- Package/public documentation:
  - `packages/hub-test-support/README.md`
  - `docs/client-protocol.md`
  - `README.md`
- Reference-only production path unless a focused bug is discovered:
  - `crates/botster-ui-contract/src/lib.rs`
  - `crates/botster-hub-client/src/lib.rs`
  - `src/runtime.rs`
  - `src/client_api.rs`
  - `src/daemon_transport.rs`

`packages/hub-test-support/package.json`, daemon protocol generation, and UI
contract generated assets should remain unchanged unless implementation
evidence proves the ticket cannot be satisfied without touching them.

If implementation discovers a focused production bug that requires editing
`src/runtime.rs`, `src/client_api.rs`, or `src/daemon_transport.rs`, Implement
must first load [[botster-runtime-reviewer-playbook]] and record the exact live
runtime-path evidence required by the Hub charter. If the repair is not a small
fix necessary for this conformance path, ask the human rather than broadening
the run.

## Risks

- A static fixture-only assertion would repeat the gap this ticket exists to
  close. The acceptance path must consume an action result produced by the real
  plugin worker.
- A live action-result assertion without applying it to scoped state would
  prove transport but not dialog/open behavior.
- Test-local hardcoded open/toggle/form-submit metadata can pass while a
  rendered control is dead. Read the user-shaped sequence from the rendered
  tree; keep only the explicitly documented negative boundary payloads
  harness-authored.
- Applying rejected or error effects would violate the typed contract and could
  hide client regressions. Assert state retention explicitly.
- A conformance helper that becomes a general renderer/store abstraction would
  broaden Hub ownership. Keep the state evaluator deliberately narrow and
  test-support-only.
- Source/crate/npm fixture mirrors and human-readable READMEs can drift. Use
  existing parity tests and asset sync/check commands.
- The current npm test can silently consume a repository sibling rather than
  its declared public dependency. Remove that bridge and prove registry-backed
  resolution before treating the package test as release evidence.
- A Rust-only report would leave Web without the promised comparison facts.
  Publish the source-derived expectations through the existing support-matrix
  JSON and keep its checksums/tests synchronized.
- The current package is versioned `0.1.12` but unpublished. A concurrent
  publication would invalidate the release assumption and requires a human
  decision before implementation continues.
- Live subprocess failures can leak hubs/workers. Retain `IsolatedHub` cleanup
  and failed-readiness reaping.
- Absolute worktree, home, or credential data must not enter committed plans,
  reports, package metadata, or fixture output.

## Acceptance checks and tests

### Focused contract and package checks

- `cargo fmt --all -- --check`
- `./test.sh -p botster-ui-contract`
  - Existing set/clear/toggle, equality, dialog presence, values/payload,
    accepted replacement, and rejected-effect validation stay green.
- `./test.sh -p botster-hub-test-support`
  - Fixture parity, report stability, action metadata extraction, scoped state
    application, and presentation binding evaluation pass.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - Rust, crate, npm fixture bytes and metadata checksums agree.
- Run
  `npm install --ignore-scripts --no-package-lock --prefix packages/hub-test-support`
  to install package dependencies from the public registry without a local
  override, then run `npm test --prefix packages/hub-test-support`.
  - Remove the pre-publication symlink block first.
  - Assert `readUiContractConformanceFixtures()` resolves
    `@trybotster/ui-contract@0.1.0` from the installed declared dependency.
  - Materialize the updated fixture and assert the new producer tokens plus the
    TypeScript-reachable support-matrix presentation facts.

### Live runtime/user-path proof

- `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - Starts a real isolated Hub with explicit worker binary, installs/enables the
    packaged fixture, renders the real snapshot, dispatches action metadata read
    from that snapshot, crosses the plugin worker, receives typed results, and
    proves:
    - accepted set opens the dialog;
    - the same result populates selected-workspace equality;
    - invalid submitted values reject with field/form errors and retain open
      state/tree;
    - valid values reach the plugin, normalize, accept, replace, and clear the
      dialog;
    - the newly added rendered toggle action and existing clear action remain
      typed and deterministic;
    - identity mismatch and malformed replacement still fail at the Hub
      boundary.
- If the implementation touches general worker dispatch, also run
  `./test.sh --test hub_lua_runtime_test`.
- Run full repository `./test.sh` after focused checks.
- Run
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`.

### Packaged artifact and release readiness

- From `packages/hub-test-support`, run
  `npm pack --dry-run --json` and `npm pack --json`; inspect the tarball list for
  the updated fixture Lua/README, metadata, generated protocol, and public
  dependency.
- Install the packed tarball into a clean temporary consumer without local or
  sibling overrides, run `verifyPackageAssets()`, materialize the fixture, and
  assert package version `0.1.12`, protocol 4, revision 19, the two live set
  producer tokens, clear/toggle cases, and the updated README claim.
- Re-run
  `npm view @trybotster/hub-test-support versions --json` before merge/release.
  Expected during implementation: latest published remains `0.1.11`.
- Do not publish in the implementation run. After merge, if operator
  credentials/2FA are required, report exactly:

  `cd packages/hub-test-support && npm publish --access public`

### Documentation and safety checks

- Review diffs in all three fixture copies and both package/source READMEs for
  stale “not supported” or incomplete matrix claims.
- Scan touched publishable files for absolute home/worktree paths, credentials,
  emails, and sibling overrides.
- Demonstrate the focused live test goes red when the plugin's set operations
  are removed or when the harness stops applying/evaluating them.

## Pipeline gates and artifacts

- Plan artifact:
  `docs/plans/prove-live-presentation-set-open-through-plugin-surface-actions.md`
- Plan gate evidence must include target routing, exact Hub charter, loaded
  notes, scope/non-scope, ownership/dependencies, release assumption, affected
  files, risks, acceptance commands, and vault-gap disposition.
- Implement evidence must include the action sequence, exact changed files,
  fixture parity/sync results, focused and full test commands, package tarball
  contents, clean-consumer install result, and registry recheck.
- Review must reject:
  - fixture-only or request-only proof;
  - hardcoded open/toggle/form-submit dispatch metadata not read from the
    rendered node (the documented negative boundary probes are exempt);
  - untyped/compatibility action paths;
  - rejected effects being applied;
  - Hub production presentation state or renderer policy;
  - hand-edited generated mirrors;
  - a second version bump or premature `0.1.12` publication;
  - local/sibling dependency overrides;
  - a Rust-only presentation report with no TypeScript-reachable support-matrix
    expectations for Web;
  - unwired report fields or stale READMEs.
- Verify must rerun the live isolated-Hub sequence and packaged clean-consumer
  proof, not rely only on implementation logs.

## Vault gaps worth capturing

- Capture after implementation if the browser-shaped test-support model proves
  a durable pattern: live conformance should apply owner-authored presentation
  effects to scoped client state and evaluate the delivered tree, not merely
  inspect action-result JSON.
- Capture if the final sequence establishes a reusable rule that rejected
  action results must preserve both presentation state and the rendered tree in
  every client conformance harness.
- Capture if retaining an already-staged unpublished package version across
  sequential producer tickets reveals a repeatable Project Pipelines release
  coordination rule.
- Do not capture speculative details before implementation proves them.

No loaded convention conflicts with this plan. It keeps UI contract ownership
standalone, Hub policy/runtime ownership explicit, browser/TUI rendering
downstream, test-support subprocess-shaped, the migration cold, and the package
release singular.
