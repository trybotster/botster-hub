# Make the live presentation dialog form-operable

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785262617_487793`
- Run: `run_1785262625_494447`
- Planned base: `origin/main` at `e3632ff`
- Assigned worktree: the Project Pipelines ticket worktree for this run
- Repository charter: [[botster-hub-playbook]]
- Role guidance: [[planner-playbook]] and [[botster-planner-playbook]]
- Surface guidance:
  [[botster-package-reviewer-playbook]] and
  [[botster-package-verifier-playbook]]
- Architecture maps:
  [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]
- Self context: [[identity]] and [[goals]]
- Hub charter notes:
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]], and
  [[webrtc bootstrap origin must be requested after the package server binds]]
- Package and conformance notes:
  [[botster is a lua plugin platform not an agent tool]],
  [[botster plugin runtime uses supervisor plus per plugin workers]],
  [[botster plugins need headless real-runtime test harnesses]],
  [[plugin conformance packages prove shared contracts while examples prove product behavior]],
  [[plugin surface registration uses injected global not require]],
  [[plugin surface handlers must validate against hub locked uinode contract]],
  [[botster package manifest validation requires hub compiled core revision]],
  [[manifest required injections must be consumed by the launched runtime]],
  [[live presentation conformance applies accepted effects before evaluating delivered trees]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[hub test support npm releases need external consumer smoke]],
  [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]],
  and [[conformance fixture revisions must be unique per published content]]
- Workflow notes:
  [[botster pipeline needs continuous product owner between agent steps]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[plan agents must author vault context as wikilinks not home paths]], and
  [[vault example paths are not repository placement conventions]]

[[project-pipelines-playbook]] is not loaded because this ticket changes no
Project Pipelines package/plugin path or workflow policy.

## Current repository facts

- `fixtures/plugins/plugin-contract-matrix/plugin.lua` authors
  `contract-app-form` as a direct child of `contract-app-panel`.
  `contract-dialog` is a sibling `presentation_if` node whose `slots.body`
  contains only `contract-dialog-body`. A blocking browser modal therefore
  hides the only form.
- The existing `run_plugin_contract_matrix_conformance` finds
  `contract-app-form` and `contract-app-message` in the raw unfiltered surface
  snapshot before or independently of modal presentation. That programmatic
  lookup can pass while a browser user cannot reach either control.
- Dialog `slots.body` accepts ordinary `UiNode` children, and Form already has
  the required action and `submit_label`. The existing UI contract therefore
  supports moving the canonical Form into the Dialog without a schema change.
- The isolated conformance path already installs and enables the copied
  package, renders it through the daemon, and dispatches actions through the
  real Hub/plugin worker. The missing proof is consumer-side visible-tree and
  blocking-modal reachability, not transport existence.
- The source fixture, Rust embedded fixture, and npm package fixture are
  byte-equal today. Rust source-parity tests and
  `packages/hub-test-support/scripts/sync-assets.mjs --check` enforce that
  relationship.
- Public npm versions currently end at
  `@trybotster/hub-test-support@0.1.12`; `0.1.13` is unallocated. The published
  `0.1.12` metadata reports conformance revision 20.

## Pinned product decisions

1. Move the one canonical `contract-app-form`, including
   `contract-app-message` and `contract-app-submit`, into
   `contract-dialog.slots.body` after the explanatory dialog text.
2. Delete the panel-level sibling form. Do not duplicate it inside the dialog,
   retain a compatibility submit path, or introduce an alternative modal
   composition.
3. Preserve all existing ids and action metadata. Nesting changes, but the set
   and count of node kinds do not: the application primitive inventory should
   remain the same unless runtime validation demonstrates otherwise.
4. Keep accepted submit behavior atomic: return `normalized_values`, apply the
   owner-authored replacement, and Clear `contract-dialog` in the same accepted
   result.
5. Keep the modal-reachability evaluator in
   `botster-hub-test-support`. It is a deliberately small conformance consumer
   over Hub-delivered trees and action results, not a renderer implementation
   or a new shared contract primitive. Moving it to `botster-ui-contract` would
   add renderer-policy gravity to the authoritative data contract and would not
   prove the real Hub/plugin-worker path.
6. Allocate conformance revision 22 and npm package version 0.1.13. Revision 22
   identifies the changed published fixture content; 0.1.13 identifies the
   immutable npm artifact. Neither value is a protocol/schema version bump.
   Because `DaemonCompatibilityRequirement::current()` derives its minimum
   conformance revision from the constant, clients built at revision 22 require
   a Hub reporting revision 22 or later. Revision 21 was allocated on `main`
   while this branch was open for the managed-Git target/worktree projection,
   so the merge resolution advances this dialog fixture to the next unique
   revision instead of reusing published-contract bytes.

These decisions are binding for Implement and Review. Choosing a duplicate
form, retaining the sibling, weakening visible-tree proof, or changing the
shared UI contract requires a new human decision rather than silent
substitution.

## Scope

### 1. Make the canonical modal composition operable

Change the canonical source fixture so the presence-bound Dialog owns the Form
and all actionable submit controls in its body. Preserve:

- the toolbar Open action and its accepted scoped `Set` operations for
  `contract-dialog` and `selected-workspace`;
- presence binding for `contract-dialog`;
- equality binding for
  `selected-workspace == "workspace-alpha"`;
- canonical `UiActionRequest` identity, `values`, and non-form `payload`;
- rejected `field_errors` keyed by `contract-app-message`, form errors, and
  unchanged tree/presentation state;
- accepted normalized values, replacement tree, and final scoped Clear;
- the independent Toggle path and existing negative identity/replacement/error
  probes.

Every user-sequence action must be read from the currently reachable rendered
node. Harness-authored payloads remain allowed only for deliberate negative
boundary probes that have no public control.

### 2. Make the conformance consumer modal-aware

Extend `run_plugin_contract_matrix_conformance` with a small browser-shaped
consumer that:

1. Starts from `ui_tree_snapshot.body`.
2. Before Open, resolves presentation predicates and proves the Dialog and its
   Form are not reachable.
3. Reads the Open action from the visible panel tree and dispatches it through
   the real daemon/plugin-worker transport.
4. Applies accepted presentation effects in package-plus-surface scope.
5. Materializes the resulting visible tree and identifies the one blocking
   Dialog.
6. Restricts actionable lookup to that Dialog subtree while it is open.
7. Finds `contract-app-form`, `contract-app-message`, and the submit action
   inside the Dialog. It must fail if the Form is reachable as a panel sibling
   or if any submit path outside the active modal remains actionable.
8. Builds the invalid and valid canonical action requests from that visible
   Form/action/input metadata.
9. After invalid submission, proves the field error targets the visible input,
   presentation state and rendered tree are unchanged, and the same Dialog and
   Form remain reachable.
10. After valid submission, proves `normalized_values`, whole-surface
    replacement application, scoped Clear removal, and a false Dialog
    predicate. It does not publish a tautological post-replacement tree check.

The helper should implement only the presentation filtering and modal
reachability needed by the conformance sequence. It must not grow DOM, focus,
layout, Ionic, Ratatui, or generic renderer policy.

### 3. Extend stable evidence

Add report fields and exact integration assertions for:

- Form and input ids reached inside the open Dialog;
- no actionable sibling Form before or during modal presentation;
- request node/action identity and invalid/valid `values`;
- rejected field-error association and Dialog/Form retention;
- accepted normalized values and replacement application;
- Clear key, scoped-state removal, and final closed state.

Keep existing node-kind, presentation Set/equality, Toggle, diagnostics,
identity mismatch, invalid replacement, package lifecycle, and settings
assertions.

### 4. Synchronize and release the package

- Update the root source fixture and its README.
- Keep the Rust embedded fixture copy byte-identical.
- Regenerate the npm fixture and metadata using the existing sync script.
- Advance `CONFORMANCE_FIXTURE_REVISION` from 21 to 22 and regenerate every
  revision-bearing published asset.
- Bump `packages/hub-test-support/package.json` exactly once to 0.1.13.
- Update root/package documentation and package tests to name 0.1.13 and
  revision 22.
- Pack and prove the exact tarball in a clean external consumer.
- Publish only from the clean merged source. If npm authentication or 2FA
  prevents publication, stop and report:

  ```sh
  cd packages/hub-test-support && npm publish --access public
  ```

## Non-scope

- No `botster-ui-contract` schema, TypeScript contract, protocol version, or
  `@trybotster/ui-contract@0.1.0` change.
- No Web/Ionic DOM renderer or TUI/Ratatui renderer implementation.
- No Core, Project Pipelines plugin, MCP, package admission, supervision,
  WebRTC, terminal data-plane, or event-lane change.
- No duplicate form, hidden sibling interaction, force-click, programmatic
  submission presented as browser proof, compatibility envelope, local
  override, sibling checkout, alternate distribution coordinate, or
  verification waiver.
- No unrelated fixture cleanup or broad presentation evaluator abstraction.

## Repository ownership and cross-repository seams

`botster-hub` owns this change because it owns the packaged conformance fixture,
the Hub/plugin-worker runtime proof, the downstream-shaped test-support crate,
generated npm assets, compatibility metadata, and release.

`botster-ui-contract` remains the unchanged authority for validating Dialog,
Form, conditional presentation, request, and result shapes. The Hub helper
consumes that contract; it does not redefine it.

`botster-web` owns real DOM rendering and click-through. This ticket explicitly
requires a browser-shaped consumer, which Hub can provide by enforcing
delivered-tree visibility and blocking-modal reachability without owning a
browser harness. The open Web ticket `ticket_1785192696_321546` separately owns
real-renderer click-through and is registered as depending on this Hub ticket
against Web target `tgt_40abcf71ccf049f4ac0c99953a799869`. It is not a
prerequisite for producing the fixed Hub fixture. If Hub's shaped consumer
passes but a real DOM consumer cannot perform the sequence, register a finding
against that Web run; do not repair Web in this run.

The open TUI-kit pin ticket `ticket_1785261259_330503` consumes the eventual
merged Hub revision and is registered as depending on this Hub ticket against
TUI-kit target `tgt_3dfae49c02454037bf13554f552baf7f`. Moving the existing
Form changes ancestry but not contract types, ids, primitive inventory, or
action semantics, so it requires no upstream TUI-kit prerequisite. Its headless
downstream proof should consume the merged Hub artifact normally. Any ancestry
assumption exposed there belongs to that consumer ticket, not a compatibility
form in Hub.

`botster-core` participates only through the Hub lockfile-pinned
`botster-session-worker` binary used for exact runtime provenance. No Core
source dependency is required.

## Assumptions and unknowns

- Assumption: one Form moved into Dialog body is the smallest contract-valid
  correction and is the ticket's preferred canonical composition.
- Assumption: a browser-shaped consumer means visibility- and modal-constrained
  interaction over the real Hub transport, not a DOM engine in this repository.
- Assumption: the sorted node-kind inventory remains byte-for-byte unchanged
  because only ancestry changes. Tests remain authoritative.
- Rebase resolution: revision 21 was globally unused during initial
  implementation, but `main` allocated it for managed-Git target/worktree
  projections before this branch merged. The combined source therefore assigns
  this dialog fixture revision 22.
- Unknown: the smallest internal representation for a filtered visible tree.
  Prefer a test-support-local recursive helper over a general renderer model.
- Unknown: npm authentication, provenance, and 2FA requirements after merge.
  They may stop publication but cannot justify a local override or waiver.

## Affected surfaces and files

- `fixtures/plugins/plugin-contract-matrix/plugin.lua`
  - move the canonical Form subtree into Dialog body;
  - remove the sibling Form.
- `fixtures/plugins/plugin-contract-matrix/README.md`
  - document modal-contained interaction and shaped-consumer proof.
- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/plugin.lua`
  and `README.md`
  - byte-identical embedded source mirror.
- `crates/botster-hub-test-support/src/lib.rs`
  - visible-tree/modal consumer, report fields, request derivation, rejection
    retention, acceptance/close evidence, and source-parity unit assertions.
- `tests/hub_daemon_lifecycle_test.rs`
  - exact report assertions, including the existing exact sorted
    `app_surface_node_kinds` vector and snapshot equality.
- `crates/botster-hub-client/src/lib.rs`
  - conformance revision 21 to 22 only.
- `packages/hub-test-support/package.json`
  - package version 0.1.13.
- `packages/hub-test-support/test.mjs`
  - package/revision assertions, exact
    `metadata.application_primitives.primitive_kinds`, support-matrix
    presentation fields, materialized fixture shape, and asset verification.
- `packages/hub-test-support/fixtures/plugin-contract-matrix/plugin.lua` and
  `README.md`
  - generated npm fixture copy.
- `packages/hub-test-support/metadata.json`
  - generated version, revision, and fixture checksums.
- `packages/hub-test-support/first-party-client-support-matrix.json`
  - generated revision and existing plugin-surface Set/equality assertions.
- Revision-bearing generated fixtures:
  `session-lifecycle-subscription-conformance-fixture.json`,
  `late-attach-history-conformance-fixture.json`, and
  `mode-flags-conformance-fixture.json`.
- `packages/hub-test-support/local-webrtc-delivery-chunk-conformance-fixture.json`
  - regenerate through the normal asset sync for parity, but do not add a
    revision field; its schema intentionally carries delivery limits and
    scenarios rather than `conformance_fixture_revision`.
- `packages/hub-test-support/README.md` and root `README.md`
  - 0.1.13/revision 22 and modal-operability contract.
- `docs/client-protocol.md`
  - document why revision 22 moves while protocol version 4 stays fixed, and
    state the resulting minimum-conformance compatibility floor.

`botster-package.json`, protocol DTO shapes, UI-contract generated assets, and
Cargo dependencies should remain unchanged.

## Risks

- Raw-tree lookup could preserve the current false-positive proof after the
  Form moves. Require all submit/input discovery after Open to use the filtered
  visible Dialog subtree, plus a negative sibling-action assertion.
- Removing the sibling could accidentally duplicate or lose ids/actions in
  generated copies. Keep one canonical subtree and enforce three-copy byte
  equality.
- Rejection could preserve raw state but lose visible field association.
  Assert the error key resolves to the still-visible input after rejection.
- Replacement could hide the Dialog and make Clear proof vacuous. Assert the
  result contains the expected Clear, scoped state removes the key, the
  predicate becomes false, and replacement is independently applied.
- A general recursive evaluator could become a second renderer. Keep it
  conformance-only and limited to existing presentation predicates plus active
  modal reachability.
- Revision 21 changes metadata bytes for unrelated generated fixtures.
  Regenerate once from the source constant and inspect the exact diff; do not
  hand-edit generated JSON or bump protocol version.
- Workspace package tests may see files omitted from the tarball. Inspect
  `npm pack`, install the actual tarball outside the repo, and repeat after
  publication.
- Stale binaries can make live proof non-authoritative. Record the merged Hub
  SHA, lockfile-pinned Core SHA, and fresh-target executable realpaths.
- npm publication is irreversible and may require 2FA. Publish only the clean,
  verified merged artifact and stop with the one operator command if blocked.

## Acceptance checks

### Focused runtime and fixture proof

```sh
./test.sh --locked --test hub_daemon_lifecycle_test \
  daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts \
  -- --exact --nocapture
./test.sh --locked -p botster-hub-test-support
```

The exact daemon test must prove install, enable, render, visible Open action,
accepted scoped Set/equality, Form reached only inside the blocking Dialog,
rejected canonical values/field errors/Dialog retention, accepted canonical
values/normalization/replacement, and final Clear/close through the real
Hub/plugin worker.

### Generated and schema parity

```sh
node packages/hub-test-support/scripts/sync-assets.mjs --check
npm test --prefix packages/hub-test-support
```

These checks must prove:

- source fixture equals the Rust embedded fixture;
- npm fixture equals the Rust emitter;
- nested Form validates through the Hub-locked schema on
  `plugin_surface_render`;
- the exact node-kind vector remains correct;
- support-matrix Set/equality fields remain correct;
- metadata and all revision-bearing assets report revision 22;
- package metadata reports 0.1.13.

### Repository gates

```sh
cargo fmt --all -- --check
./test.sh --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
git diff --check
```

Every failure requires exact attribution. There is no pre-existing-failure
blanket waiver.

### Packaged and downstream proof

From `packages/hub-test-support`:

```sh
npm pack --dry-run --json
npm pack --json
```

Install the actual tarball in a clean temporary Node consumer outside the
repository. Import the package, assert version 0.1.13 and revision 22, run
`verifyPackageAssets()`, materialize the plugin-contract-matrix fixture, and
use balanced Lua-table containment to assert the shipped source has the Form
inside the Dialog with no sibling Form. Show the same check red with a
structurally valid sibling-Form ablation.

After merge, repeat against
`@trybotster/hub-test-support@0.1.13` and record the public
`dist.integrity`. Record the merged Hub SHA, lockfile-pinned Core SHA, exact
build/test commands, and fresh-target binary realpaths. No local or sibling
override is valid evidence.

Scan committed and packed artifacts for absolute local paths, usernames,
emails, tokens, npm credentials, and non-fixture endpoints.

## Pipeline gates and artifacts

- Plan artifact: this document plus the Project Pipelines `plan` artifact.
- Implement artifact: changed-file rationale, exact focused/full command
  results, generated parity evidence, revision/version allocation evidence,
  and no-waiver statement.
- Review gate: no open correctness, architecture, reachability, parity,
  packaging, or ownership findings.
- Verify artifact: exact merged-source runtime provenance, tarball and clean
  consumer evidence, public package evidence or the one 2FA-blocked operator
  command.

The run vault checklist records loaded notes, no convention conflict, deferred
implementation commands with no waiver, and the vault capture disposition.

## Vault gaps worth capturing

- Modal conformance consumers must derive actionable controls from the
  currently visible delivered tree and enforce blocking-dialog reachability;
  locating hidden siblings in an unfiltered tree is not user-path proof.
- [[live presentation conformance applies accepted effects before evaluating delivered trees]]
  should be sharpened after implementation: predicate evaluation alone is
  insufficient when the visible modal composition makes required controls
  unreachable.
- Capture only after implementation confirms the modal-aware helper is a
  reusable conformance pattern rather than a one-fixture detail.
