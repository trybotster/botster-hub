# Publish hub test-support application-primitives fixture for web consumers

## Context Loaded

- Ticket `ticket_1783534885_466538`, run `run_1783534900_917729`, step `botster_plan`, gate `botster_plan_gate`.
- Pipeline context contained no prior artifacts, findings, reviews, open questions, or answers.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]], [[identity]], [[goals]].
- Botster planning context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Relevant fixture/package conventions: [[botster-web should import canonical core uinode fixtures instead of mirroring them]], [[botster first party client support matrices belong in hub test support]], [[hub test support npm releases need external consumer smoke]], [[plan steps need reviewable plan artifacts]].
- Repo evidence inspected:
  - `fixtures/plugins/plugin-contract-matrix/plugin.lua`, `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/plugin.lua`, and `packages/hub-test-support/fixtures/plugin-contract-matrix/plugin.lua` already contain the application primitive composite under `contract.app`.
  - `contract.app` includes `panel`, `toolbar`, `metric_grid`, `metric`, `section`, `status_badge`, `table`, and `empty_state`.
  - `crates/botster-hub-test-support/src/lib.rs` already asserts the validated app surface node kinds through `run_plugin_contract_matrix_conformance`.
  - `tests/hub_daemon_lifecycle_test.rs` already asserts the same node-kind list through a real isolated daemon fixture test.
  - `docs/client-protocol.md` documents that expanding the plugin contract matrix to application primitives increments `CONFORMANCE_FIXTURE_REVISION` without changing `PROTOCOL_VERSION`.
  - `packages/hub-test-support/package.json` is still `0.1.1` and the public Node API names only `pluginContractMatrix`, not an explicit application-primitives fixture route.
  - `packages/hub-test-support/README.md` still tells consumers to install `@trybotster/hub-test-support@0.1.1`.

## Scope

- Publish the existing hub-validated `contract.app` application-primitives composite as an explicit downstream-consumable fixture surface from `packages/hub-test-support`.
- Add clear Node API names for the fixture, for example:
  - `applicationPrimitivesFixturePath()`
  - `materializeApplicationPrimitivesFixture(destination)`
  - metadata fields that name the fixture package, surface id, expected primitive kinds, and source artifact.
- Preserve the existing plugin-contract-matrix API as the backing fixture package; the explicit application-primitives API may be an alias over that fixture, but downstream consumers should not have to infer that `contract.app` is the desired primitive fixture.
- Update TypeScript declarations, package metadata, checksum verification, and package exports/files as needed so the application-primitives fixture is included in the packed/published artifact.
- Bump the npm package version to a new exact version, likely `0.1.2`, unless implementation discovers a repo release policy requiring a different next version.
- Update package docs and `docs/client-protocol.md` with exact botster-web and botster-tui consumer instructions:
  - package spec to install or local path fallback if publishing is unavailable;
  - API/fixture name to import;
  - `contract.app` / application-primitives surface id to render or inspect;
  - expected primitive kinds.
- If npm publishing is available, publish the new version and record the registry tarball/integrity. If publishing is not available, produce a durable local package route such as `npm pack` tarball path and exact `file:` dependency guidance for the downstream web ticket.
- Keep all schema authority in core/hub validation and generated artifacts; do not introduce a handwritten duplicate UiNode schema in Node or hub tests.

## Non-Scope

- No new UiNode primitive definitions, aliases, or validation logic in hub.
- No botster-web or botster-tui renderer implementation in this repo.
- No daemon protocol redesign, protocol version bump, or new daemon request/response type unless generated protocol output has independently changed.
- No separate fixture package duplicating `plugin-contract-matrix`.
- No broad cleanup of package publishing infrastructure or historical plans.

## Assumptions And Unknowns

- Assumption: the application-primitives composite already accepted in this checkout is the intended authoritative fixture; implementation should not replace it with a second fixture unless the current one fails validation.
- Assumption: a new npm version is required because downstream currently resolves `@trybotster/hub-test-support@latest` as `0.1.1`.
- Assumption: `toolbar` is the current core spelling for the action-bar primitive in this fixture; do not add `action_bar` as a hub-side alias.
- Assumption: Node consumers need a fixture package path/materializer and metadata, not a runtime daemon helper.
- Unknown: whether npm publish credentials are available in the pipeline. The fallback is a packed tarball and exact `file:` dependency/import guidance.
- Unknown: whether `CONFORMANCE_FIXTURE_REVISION` already reflects the landed primitive expansion. Implementation should verify it does not need another bump for any additional observable fixture export change.
- Worktree/target assumption: this run is bound to target `tgt_7e208a0c76a44980a83b63af976b1f22` and branch `project-pipelines/ticket_1783534885_466538`.

## Affected Surfaces And Files

- Botster layers touched: hub test-support crate metadata emitter, Node test-support package, generated/package assets, docs, release report.
- Primary Node package files:
  - `packages/hub-test-support/package.json`
  - `packages/hub-test-support/index.js`
  - `packages/hub-test-support/index.d.ts`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/test.mjs`
  - `packages/hub-test-support/README.md`
  - `packages/hub-test-support/scripts/sync-assets.mjs`
- Rust source for package metadata generation:
  - `crates/botster-hub-test-support/src/lib.rs`
  - `crates/botster-hub-test-support/examples/node_package_assets.rs`
  - `crates/botster-hub-client/src/lib.rs` only if conformance revision changes.
- Fixture assets to keep synchronized, not forked:
  - `fixtures/plugins/plugin-contract-matrix/**`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/**`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
- Docs/reports:
  - `docs/client-protocol.md`
  - a new implementation report under `docs/reports/` with publish or local-tarball consumer instructions.

## Risks

- Downstream ambiguity risk: exposing only `materializePluginContractMatrixFixture` leaves botster-web guessing which surface is the application-primitives contract. Add explicit fixture/API naming.
- Publication drift risk: local package assets can pass while the registry still serves `0.1.1`. Require external consumer proof from the actual published coordinate or a packed tarball fallback.
- Duplicate schema risk: metadata can become a second UiNode schema if it encodes prop requirements. Keep metadata to fixture identity and observed primitive kind names; validation remains core-owned.
- Versioning risk: updating docs without bumping/publishing leaves `@latest` unusable for web. Treat version/publish or tarball handoff as acceptance-critical.
- False runtime proof risk: package import alone does not prove hub validation. Keep the existing real daemon conformance test in the acceptance set.
- Export-map risk: files may exist in the checkout but be hidden from consumers by `exports` or omitted by `files`. Prove with `npm pack --dry-run` and clean consumer install.

## Acceptance Checks And Tests

- Asset sync and package API:
  - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - `node packages/hub-test-support/test.mjs`
  - The Node package test should import the explicit application-primitives API, materialize the fixture, assert `metadata.application_primitives.surface_id === "contract.app"`, and assert the primitive kind list includes `metric`, `metric_grid`, `status_badge`, `toolbar`, `table`, `empty_state`, `section`, and `panel`.
- Rust test-support parity:
  - `./test.sh -p botster-hub-test-support`
  - Required if metadata emitter, fixture checks, or test-support source changes.
- Hub-client/protocol gate:
  - `./test.sh -p botster-hub-client`
  - Required if `CONFORMANCE_FIXTURE_REVISION`, generated protocol, or client metadata changes.
- Real daemon validation proof:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - Must continue proving the composite reaches `plugin_surface.body` and `plugin_surface.ui_tree_snapshot.body` through the daemon `PluginSurfaceRender` path and core `UiNode::validate()`.
- Package inclusion proof:
  - `npm pack --dry-run --json` from `packages/hub-test-support`, preferably with a temp cache.
  - Confirm README, metadata, declarations, API, daemon protocol, and fixture files are included.
- External consumer proof:
  - If published: install `@trybotster/hub-test-support@<new-version>` in a clean temp consumer and assert package version, explicit application-primitives API, `verifyPackageAssets()`, fixture materialization, and primitive tokens.
  - If not published: install the packed tarball in a clean temp consumer and include the tarball path plus `file:` dependency guidance in the report/gate.
- Documentation proof:
  - Docs name the exact package version or tarball route and the exact API/surface id for botster-web and botster-tui.
  - Docs preserve `ui_tree_snapshot` as the blessed rendering path.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/publish-hub-test-support-application-primitives-fixture-for-web-consumers.md`.
- Checklist: `checklist_1783534932_294364`.
- Implement gate should attach:
  - changed files;
  - package version and publish/tarball coordinate;
  - exact consumer import/API guidance;
  - sync/check/test command outputs;
  - clean external consumer proof;
  - explicit confirmation that no duplicate UiNode schema was added.
- Review should reject:
  - docs that leave web/TUI consumers guessing between package paths or fixture surfaces;
  - package version/docs mismatches;
  - local-only proof without registry or tarball consumer evidence;
  - hidden package files due to `exports`/`files` omissions;
  - any hub-local schema or primitive alias added for this ticket.

## Vault Gaps Worth Capturing

- Capture after implementation if the explicit application-primitives fixture API becomes the durable pattern for exposing individual conformance surfaces from a broader fixture package.
- Capture if Project Pipelines settles a release fallback convention for handing packed npm artifacts between dependent tickets when real publishing is unavailable.
- Capture if support metadata should consistently distinguish fixture package name, application surface id, primitive kind inventory, and renderer entrypoint.
- No convention conflict found. The plan follows core-owned UiNode validation, hub-test-support as the downstream fixture boundary, external consumer smoke requirements, and repo-visible plan artifact discipline.
