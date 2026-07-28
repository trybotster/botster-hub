# UiNode contract extraction implementation report

## Target

- Repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785192683_691772`
- Run: `run_1785192799_725300`

The run context and approved plan both route this work to the same Hub target.
All edits were made in the run worktree on
`project-pipelines/ticket_1785192683_691772`.

## Guidance applied

- `[[implementer-playbook]]`
- `[[botster-implementer-playbook]]`
- `[[botster-hub-playbook]]`
- `[[botster-hub-client-playbook]]`
- `[[project-pipelines-playbook]]`
- The approved plan at
  `docs/plans/extract-uinode-into-botster-ui-contract.md`
- Targeted notes covering product gravity, consumer proof, Hub-locked
  validation, client-local modal/presentation state, automatic dialog
  presentation, schema-owned props, action labels, form placeholders,
  cold-turkey migrations, TypeScript optionality and symmetric drift checks,
  adapter translation, conformance fixture authority/revisions/READMEs,
  runtime-delivered snapshots, external npm smoke tests, subprocess Hub proof,
  repository test wrappers, strict lints, negative controls, implementation
  reports, PR linkage, and checklist evidence.

The ticket's explicit sibling-contract-package decision supersedes the Hub
charter's general exclusion of shared contracts for this surface. The new
contract remains separate from Hub runtime and the daemon client crate.

## Implementation

- Added the self-contained `botster-ui-contract` crate with the complete pinned
  UiNode vocabulary and validation matrix, a contract-owned request id, scoped
  presentation predicates/operations, explicit Form submit labels, and direct
  accepted-result replacement trees. The crate has no Core, Hub runtime,
  renderer, Lua, browser, TUI, or marketplace dependency.
- Added the generated public `@trybotster/ui-contract@0.1.0` npm package with
  strict-compiling TypeScript declarations, JSON Schema, shared fixtures, and
  deterministic generate/check commands.
- Cold-switched Hub runtime, local client API, daemon transport, and
  `botster-hub-client` to the canonical typed request/result/tree contract.
  Protocol version is 4 and conformance revision is 19. The removed split
  action tuple and `UiTreeUpdateRef`/`tree_update` do not have compatibility
  decoders.
- Hub runtime continues to validate and route trusted plugin output. It checks
  request, surface, action, and node correlation on results, but stores no
  presentation state and applies no renderer policy.
- Updated the Project Pipelines example and the canonical plugin contract
  matrix to read form drafts from `values`, metadata from `payload`, require a
  submit label, retain rejected state, and return accepted clear/replacement
  effects. Regenerated the Rust and npm fixture mirrors.
- Bumped `@trybotster/hub-test-support` to unused version `0.1.12`, pinned the
  normal `@trybotster/ui-contract` dependency, imported shared UI fixtures at
  runtime, and regenerated protocol/support metadata without copying UI types
  or fixtures.
- Audited `examples/synthetic-plugin/**`; it declares only MCP and timer
  capabilities and authors no UI surface, so no migration was required.

### Implement review remediation

- Replaced hand-maintained TypeScript/schema string unions with generated
  values from an exhaustive Rust enum inventory. Every exported string enum is
  checked across Rust serialization, TypeScript, and JSON Schema; adding a Rust
  variant without updating the inventory is a compile error. This also removed
  the five iframe sandbox tokens that Rust intentionally rejects.
- Extended the canonical plugin matrix with a dialog bound to scoped
  `contract-dialog` presence and a selected-workspace equality binding. The
  real daemon proof inspects the validated typed snapshot, verifies the
  accepted clear targets that rendered dialog key, and verifies the replacement
  node.
- Added real worker branches and Hub assertions for mismatched result identity
  and an accepted malformed replacement. Both are rejected as
  `invalid_action_result` at `plugin_surface_action`.
- Added exact contract error assertions for non-accepted effects, blank
  presentation operation keys, invalid replacement trees, and blank
  presentation predicate keys. Removed obsolete same-type re-export
  tautologies and kept tests that exercise actual serialization/validation.
- Made the checked-in hub-test-support npm test assert its UI dependency
  metadata, resolve the local package through its production package name, load
  the shared fixtures, and verify a known dialog fixture.
- Corrected the published README fixture key, documented result identity
  correlation, and changed the example TUI text to distinguish emitted
  contracts from separately routed renderer adoption.

## Files changed

- Workspace and authority:
  `Cargo.toml`, `Cargo.lock`, `crates/botster-ui-contract/**`,
  `packages/ui-contract/**`.
- Production/runtime protocol:
  `src/runtime.rs`, `src/client_api.rs`, `src/daemon_transport.rs`,
  `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`,
  `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Conformance and publication support:
  `crates/botster-hub-test-support/**`,
  `packages/hub-test-support/**`,
  `fixtures/plugins/plugin-contract-matrix/**`.
- First-party example, tests, and current documentation:
  `examples/project-pipelines/**`, `tests/hub_lua_runtime_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, `README.md`,
  `docs/client-protocol.md`, and this report.

## Ownership and cross-repository boundaries

- Renderer-neutral vocabulary, validation, generated TypeScript/schema, and
  shared fixtures are owned by `botster-ui-contract` /
  `@trybotster/ui-contract`.
- Hub owns validation, package/surface/action routing, diagnostics, and daemon
  projection only. Client renderers retain scoped presentation storage and
  layout/focus policy.
- `botster-hub-client` owns daemon framing and typed references, not a copied or
  re-exported UI contract. Hub test support owns transport/harness proof and
  consumes the standalone package normally.
- No Core, Web, TUI, TUI kit, Workspaces, or external Project Pipelines
  repository was edited. The external Project Pipelines adoption remains
  separately routed as `ticket_1785194090_628084`, blocked by
  `dependency_1785194093_410838`. Web/TUI adoption and Core deletion remain
  their separately routed tickets.

## Deviations and assumptions

- No scope deviation from the approved plan.
- Registry verification on 2026-07-27 showed hub-test-support versions only
  through `0.1.11` and an npm 404 for `@trybotster/ui-contract`; therefore
  `0.1.12` and `0.1.0` remain the chosen unused versions.
- Presentation keys are author-visible local keys. Hub/package/surface scope is
  supplied by the admitted client context, not encoded as author-controlled
  scope in an operation.

## Verification and downstream proof

- Final `cargo test -p botster-ui-contract`: 71 substantive contract tests and
  3 generated-asset tests passed after removing tautologies.
- `./test.sh -p botster-hub-client`: 41 unit tests and 4 doc tests passed.
- `./test.sh -p botster-hub-test-support`: 32 unit tests and 3 doc tests passed.
- `./test.sh --test hub_client_api_test`: 23 passed.
- `./test.sh --test hub_lua_runtime_test`: 18 passed, including the real
  Project Pipelines worker action path.
- `./test.sh --test hub_daemon_lifecycle_test`: 100 passed and the documented
  32-session local stress case remained ignored.
- `./test.sh --test hub_test_support_conformance_test`: 2 passed.
- Full repository `./test.sh`: passed.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, and `git diff --check`: passed.
- UI generation check, both npm package tests, and the hub-test-support asset
  sync check passed. The hub-test-support test now imports and asserts the
  standalone UI fixtures through `@trybotster/ui-contract`.
- Final `npm pack --dry-run --json` and `npm pack --json` passed for both
  packages. The UI tarball contains 7 declared files; hub-test-support contains
  15 and declares the exact normal UI dependency.
- A clean temporary consumer installed both final tarballs, loaded the shared
  UI fixture through hub-test-support, verified checksums and protocol
  4/revision 19, and compiled the UI plus daemon declarations with strict
  TypeScript 7.0.2.
- Negative control: temporarily disabling accepted-only effect validation made
  `ui_action_result_applies_accepted_presentation_and_inline_replacement` fail
  on the rejected-result assertion; restoring the guard made the same focused
  test pass.
- Enum ablation control: temporarily adding `UiNodeKind::AblationProbe` without
  inventory coverage made `cargo check -p botster-ui-contract` fail at the
  generated asset inventory's exhaustive match; restoring the enum made the
  check pass.
- The focused real-daemon plugin matrix test passed with snapshot-level dialog
  presence/equality assertions and worker-path identity/replacement rejection.
- One pre-existing sub-second timing assertion in
  `shutdown_rejects_unrelated_failure_without_waiting_for_live_daemon` failed
  once at 1.15 seconds after passing earlier. The complete test-support crate
  immediately passed on rerun, and the later full repository wrapper passed.

The production path proved is daemon request → `HubClientApi` → `HubRuntime` →
plugin worker → `botster-ui-contract` validation → typed daemon response.

## Residual risk and unverified behavior

- Neither npm package was published; publication remains an operator action
  after merge. Publish `packages/ui-contract` first with
  `npm publish --access public`, then `packages/hub-test-support` with the same
  command.
- Protocol-3 clients intentionally reject this Hub until their routed adoption
  tickets consume the new artifacts.
- Renderer click/keyboard behavior and client-local presentation application
  are intentionally unverified here and belong to Web/TUI target runs.
- Core still contains its historical UI module until the separately routed
  removal ticket lands.

## Missing vault guidance

Implementation confirmed two durable gaps: the exact typed
set/clear/toggle/presence/truth/equality and accepted replacement wire shape,
and the package-boundary rule that a standalone shared contract owns canonical
fixtures while transport test support only pins and consumes it. No vault files
were changed from this repository-scoped run; these gaps are recorded here for
later vault capture.
