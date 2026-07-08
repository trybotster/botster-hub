# Update hub UiNode validation and test support for application primitives

## Context Loaded

- Ticket `ticket_1783529011_789836`, run `run_1783531677_984608`, step `botster_plan`, gate `botster_plan_gate`.
- Dependency `ticket_1783529011_837869` is closed: "Add UINode application primitives and semantic interaction contracts".
- No prior artifacts, findings, questions, or answers were present in the current pipeline context.
- Required self/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]].
- Botster planning context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[prefer framework and library components over custom solutions]].
- Repo evidence inspected:
  - `Cargo.toml` tracks `botster-core` and `botster-core-daemon` from `trybotster/botster-core` branch `main`; `Cargo.lock` currently pins both at `e954f28e2aad41841d9334ee327898b65c5a7537`.
  - `git ls-remote https://github.com/trybotster/botster-core refs/heads/main` returned upstream `978c436865c215828b02a8b0fcca5f8d89413e96`, so this checkout is behind current core main.
  - Local locked `botster-core` source at `e954f28` has `UiNodeKind` entries for `panel`, `table`, and `empty_state`, but not the ticket's newer `metric_grid`, `toolbar` or `action_bar`, `status_badge`, or `section`.
  - `src/runtime.rs` production render path deserializes plugin surface output into `botster_core::UiNode` and calls `node.validate()`. That is the validation authority to preserve.
  - `src/client_api.rs` routes `HubClientRequest::PluginSurfaceRender` through `HubRuntime::render_plugin_surface`.
  - `src/daemon_transport.rs` maps daemon `plugin_surface_render` requests into that client API and serializes validated output as `DaemonPluginSurface` with `ui_tree_snapshot`.
  - `fixtures/plugins/plugin-contract-matrix`, `crates/botster-hub-test-support/fixtures/plugin-contract-matrix`, and `packages/hub-test-support/fixtures/plugin-contract-matrix` publish the shared fixture surface; currently `contract.app` is a small panel/text/button tree.
  - `crates/botster-hub-test-support/src/lib.rs` has `run_plugin_contract_matrix_conformance`, including app render, empty render, blocked render, invalid body validation, settings, configuration, action, and package route checks.
  - `tests/hub_daemon_lifecycle_test.rs` already proves the fixture through a real isolated hub subprocess and daemon protocol.
  - `docs/client-protocol.md` and fixture READMEs document `plugin_surface.ui_tree_snapshot` as the blessed browser/TUI render contract.
- Project Pipelines checklist evidence:
  - Run checklist `checklist_1783531730_143588` was created and all four vault workflow items were marked done.
  - Checklist evidence records notes read, no convention conflicts, repo inspection commands, and no durable vault capture yet.

## Scope

- Update the locked `botster-core` and `botster-core-daemon` revisions together if needed so hub compiles against the core revision that contains the new UiNode application primitives.
- Keep the existing validation boundary: plugin output must be deserialized as `botster_core::UiNode` and accepted or rejected by core `UiNode::validate()`.
- Expand the hub-owned plugin contract matrix app surface so a single composite app screen includes the ticketed primitives:
  - `metric_grid`
  - `table`
  - `toolbar` or `action_bar`, depending on the exact core vocabulary after dependency update
  - `empty_state`
  - `status_badge`
  - `section` and `panel`
- Update all published/shared copies of the plugin contract matrix fixture and any generated package metadata or checksums that embed it.
- Extend hub tests and `botster-hub-test-support` conformance assertions to prove the composite surface passes through the real daemon `plugin_surface_render` runtime path and exposes the same validated tree through `plugin_surface.body` and `plugin_surface.ui_tree_snapshot.body`.
- Keep the invalid-body/unknown-kind tests in place so the same path still rejects bad payloads via core validation.
- Update docs/examples that describe the plugin contract matrix and supported UiNode primitives.

## Non-Scope

- No hub-specific visual layout policy for the new primitives.
- No browser, TUI, Catalyst, Ionic, or renderer adapter implementation.
- No hand-maintained duplicate UiNode schema in Rust tests, Lua fixtures, TypeScript, README prose, or generated client artifacts.
- No new daemon request type, protocol redesign, package manifest vocabulary, or plugin workflow policy.
- No broad cleanup of existing fixture surfaces or historical `docs/plans` files beyond changes required by this ticket.
- No compatibility shim that accepts old and new primitive names if the core contract has settled on one vocabulary.

## Assumptions And Unknowns

- Assumption: the closed dependency landed the new primitives on `botster-core` main; implementation should confirm by updating/fetching core and inspecting the authoritative `UiNodeKind`/validation contract before editing fixtures.
- Assumption: both `botster-core` and `botster-core-daemon` should stay on the same core repository revision in `Cargo.lock`.
- Assumption: if the core vocabulary offers both `toolbar` and `action_bar`, the fixture should use the public application-screen primitive intended by the dependency ticket and document the chosen spelling.
- Assumption: `status_badge` is distinct from existing `badge`; if core instead models status through a prop on `badge`, implementation must ask for plan review or a human answer before silently substituting.
- Unknown: whether the dependency update changes generated daemon protocol DTOs. If it does not, generated TypeScript may remain byte-identical except for fixture/test-support package assets.
- Unknown: whether changing the conformance fixture primitive mix requires bumping `CONFORMANCE_FIXTURE_REVISION`. Prefer bumping if downstream fixture expectations can observe different supported primitive coverage.
- Unknown: exact required props for the new core primitives until the updated core schema is inspected. Use the smallest valid props that prove admission without encoding renderer layout policy.

## Affected Surfaces And Files

- Botster layers touched: Rust hub runtime validation dependency, daemon protocol conformance tests, hub test-support crate, Node test-support package assets, plugin fixture docs/examples.
- Production validation path to preserve and prove:
  - `src/daemon_transport.rs`
  - `src/client_api.rs`
  - `src/runtime.rs`
- Dependency artifacts:
  - `Cargo.lock`
  - possibly `Cargo.toml` only if the dependency spec itself must change; prefer lockfile-only revision movement if branch tracking remains correct.
- Fixture and test-support assets:
  - `fixtures/plugins/plugin-contract-matrix/plugin.lua`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/plugin.lua`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/README.md`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/plugin.lua`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/README.md`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/daemon-protocol.ts` only if generated DTO output changes.
- Rust conformance/test files:
  - `crates/botster-hub-test-support/src/lib.rs`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `crates/botster-hub-client/src/lib.rs` only for conformance revision or DTO changes.
  - `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts` only if DTO generation changes.
- Docs:
  - `docs/client-protocol.md`
  - fixture READMEs listed above.

## Risks

- Dependency drift risk: updating only one of `botster-core` and `botster-core-daemon` can produce split contracts. Keep both locked to the same upstream revision.
- Schema duplication risk: tests can accidentally become a second schema if they assert large prop sets or reconstruct validation rules. Assert representative node ids/types and trust core validation for schema validity.
- False-positive risk: a pure DTO or fixture-copy test would not prove the production user path. Acceptance must include real daemon `PluginSurfaceRender` through `HubRuntime::render_plugin_surface`.
- Renderer-policy leakage risk: a rich composite fixture can smuggle web/TUI layout preferences into hub. Use simple semantic nodes and avoid visual placement policy beyond nesting needed for a valid UiNode tree.
- Fixture publication drift: source fixture, Rust crate embedded fixture, Node package fixture, metadata checksums, and docs can diverge unless regenerated or synchronized deliberately.
- Conformance compatibility risk: downstream clients may compare fixture ids or revision. If observable fixture expectations change, bump and document conformance revision.
- Unknown primitive spelling risk: `toolbar` vs `action_bar` and `status_badge` vs `badge` must follow core exactly; do not paper over a mismatch with aliases in hub.

## Acceptance Checks And Tests

- Dependency proof:
  - `cargo update -p botster-core -p botster-core-daemon`
  - Confirm `Cargo.lock` pins both packages to the same upstream revision and that the updated core source contains the requested application primitives.
- Focused hub-client/protocol gate:
  - `./test.sh -p botster-hub-client`
  - Required if `CONFORMANCE_FIXTURE_REVISION`, daemon DTOs, or generated TypeScript are touched.
- Focused test-support gate:
  - `./test.sh -p botster-hub-test-support`
  - Must prove embedded fixture assets and package metadata/checksums are current.
- Real daemon fixture proof:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - Must prove a real isolated hub installs/enables the fixture, renders `contract.app`, and returns the composite app surface through `plugin_surface.body` and `plugin_surface.ui_tree_snapshot.body`.
- Validation rejection proof:
  - The existing invalid-body path must still return `DaemonResponseKind::OperatorError`, `error.code == "invalid_surface"`, `error.operation == "plugin_surface_render"`, and a diagnostic for `plugin_surface_render`.
- Runtime/user-path evidence required in review:
  - Show the request travels through daemon `PluginSurfaceRender`, `HubClientApi`, `HubRuntime::render_plugin_surface`, core `UiNode::validate()`, and daemon response serialization.
  - Show no hub-local enum, JSON schema, TypeScript schema, or Lua validator for the new primitive set was added.
- Documentation/artifact check:
  - Docs or fixture README mention the new supported UiNode primitives and preserve `ui_tree_snapshot` as the blessed rendering path.
  - If Node package assets are regenerated, `packages/hub-test-support/test.mjs` or equivalent asset verification should pass.
- Broader safety gate if dependency update has wider compile impact:
  - `./test.sh`
  - If full suite is too costly or fails outside touched surfaces, record exact failing tests and why they are unrelated.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/update-hub-uinode-validation-and-test-support-for-application-primitives.md`.
- Run checklist: `checklist_1783531730_143588`.
- Worktree/target assumptions:
  - Current branch is `project-pipelines/ticket_1783529011_789836`.
  - Current worktree is the pipeline-assigned worktree for `target_id` `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Implement gate should attach:
  - changed files,
  - updated core revision evidence,
  - focused test command outputs,
  - generated/test-support asset sync evidence,
  - docs/examples updated or explicit proof they did not need changes.
- Review should reject:
  - duplicate UiNode schemas,
  - renderer policy in hub,
  - tests that only construct DTOs without real daemon render,
  - unsynchronized fixture copies or stale metadata,
  - dependency revisions that leave core and core-daemon on different commits.

## Vault Gaps Worth Capturing

- Capture after implementation if a durable convention emerges for proving new core UiNode primitives in consuming repos through a hub-owned composite fixture rather than renderer-specific tests.
- Capture if conformance fixture revision policy is clarified for primitive-coverage-only fixture changes.
- Capture if core settles naming guidance for `toolbar` vs `action_bar` or `status_badge` vs existing `badge`, because future plugin authors will otherwise rediscover the distinction.
- No convention conflict found. The plan follows the hub-client external boundary, core-owned UiNode validation, generated artifact drift checks, plugin fixture ownership, and the narrow hub role.
