# Expose reusable plugin UI conformance harness from botster-hub-test-support

## Context Loaded

- Ticket `ticket_1783280111_645888`, run `run_1783286888_665900`, step `botster_plan`, gate `botster_plan_gate`.
- Required playbooks and self context: [[planner-playbook]], [[botster-planner-playbook]], [[identity]], [[goals]].
- Botster planning context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[plan agents must author vault context as wikilinks not home paths]].
- Artifact/checklist discipline: [[plan steps need reviewable plan artifacts]] and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo evidence inspected:
  - `fixtures/plugins/plugin-contract-matrix/{botster-package.json,plugin.lua,README.md}` already provides the hub-owned fixture package from the closed dependency.
  - `tests/hub_daemon_lifecycle_test.rs` already proves the fixture through real daemon install, enable, list, show, render, action, configuration, and error paths, but that proof is embedded in an internal hub integration test.
  - `crates/botster-hub-test-support/src/lib.rs` already exposes `IsolatedHubBuilder`, `run_client_conformance`, `run_project_pipelines_conformance`, `run_foreground_terminal_app_open_conformance`, the first-party support matrix, and stable conformance report structs.
  - `docs/client-protocol.md` documents downstream use of `botster-hub-client` and `botster-hub-test-support`, but only names the optional Project Pipelines plugin helper for plugin surface/action coverage.
  - `crates/botster-hub-client/src/lib.rs` exposes the public daemon DTOs needed by the harness: package routes/configuration, `DaemonPluginSurface`, `PluginSurfaceRender`, `PluginSurfaceAction`, and package configuration requests.

Checklist note: `project_pipelines_create_vault_checklist` timed out at the Project Pipelines plugin worker boundary. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist-equivalent evidence is preserved in this plan artifact and should also be copied into gate evidence.

## Scope

- Add a reusable, public-contract oriented plugin UI conformance helper in `crates/botster-hub-test-support`.
- Base the helper on the existing checked-in fixture package at `fixtures/plugins/plugin-contract-matrix`; do not create a second fixture with similar semantics.
- Expose a stable report type, likely `PluginUiConformanceReport`, with enough structured fields to distinguish:
  - producer contract success/failure for package descriptors, declared routes, surface identity, UiNode payload shape, action result shape, and configuration round trips;
  - client/rendering failure hooks by returning precise report fields that downstream clients can compare against their own renderer output;
  - environment/setup failures through existing `IsolatedHubError::diagnostic` and `ConformanceError` operations.
- Add a helper such as `run_plugin_ui_conformance(&IsolatedHub, package_path)` or `run_plugin_contract_matrix_conformance(&IsolatedHub, package_path)` that:
  - installs and enables the local fixture through daemon requests;
  - asserts package route descriptors and app/settings surfaces;
  - calls `plugin_surface_render` for app, empty, blocked, and settings surfaces;
  - verifies `DaemonPluginSurface.package_name`, `surface_id`, and `body`;
  - calls `plugin_surface_action` for success and error paths;
  - sets package configuration with valid and invalid values and verifies redacted effective values;
  - fails fast with operation-specific `ConformanceError` values when required metadata or diagnostics are missing.
- Refactor `tests/hub_daemon_lifecycle_test.rs` so the existing real contract-matrix integration path uses the public helper, while keeping any hub-internal assertions only where they prove hub-specific behavior not suitable for downstream harness consumers.
- Update docs so hub, web, TUI, and first-party plugin developers have exact commands and helper usage for the full plugin contract matrix against an isolated hub.
- Keep all fixture/docs/report values synthetic and PII-free.

## Non-Scope

- No new plugin runtime primitives, new daemon request types, protocol version bump, or package manifest vocabulary.
- No browser SPA, TUI renderer, Rails, old monolith, or Project Pipelines product behavior changes.
- No private hub runtime coupling from `botster-hub-test-support`; the crate should continue to depend on `botster-hub-client` and spawn a real hub subprocess.
- No broad rewrite of `run_project_pipelines_conformance`; it can remain as the product-specific helper while the new helper becomes the generic fixture-backed UI contract harness.
- No downstream botster-web or botster-tui test implementation in this ticket. The hub repo should publish the reusable harness and document how those clients consume it.

## Assumptions And Unknowns

- Assumption: the harness should accept an explicit `package_path`, with docs passing `fixtures/plugins/plugin-contract-matrix`, rather than trying to discover the repo checkout path from inside the library.
- Assumption: "one command or documented helper" can be satisfied by a compile-checked helper plus a documented `cargo test` pattern that starts an isolated hub using explicit binary paths.
- Assumption: the current `ConformanceError` enum can be extended for missing routes, missing package rows, diagnostics, action states, and configuration values instead of introducing a parallel error hierarchy.
- Assumption: producer contract failures should be reported as harness errors with operation names like `contract_matrix_routes`, `contract_matrix_render_app`, or `contract_matrix_config_invalid`, while downstream client rendering failures remain comparisons against the report payloads.
- Unknown: whether implementers will choose to keep a smaller direct daemon fixture test after extracting the helper. If the helper fully covers the path and the daemon test invokes it, duplicate direct assertions should be removed rather than maintained in parallel.
- Unknown: whether docs should live only in `docs/client-protocol.md` plus fixture README, or also in the root README. Prefer the two narrower docs first unless implementers find a root README conformance section already owns this audience.

## Affected Surfaces And Files

- Botster layers touched: Rust hub test-support crate, external hub-client daemon protocol consumption, checked-in plugin fixture docs, hub daemon integration tests, client protocol docs.
- Primary files:
  - `crates/botster-hub-test-support/src/lib.rs`
  - `crates/botster-hub-test-support/Cargo.toml` only if a required public helper dependency is missing; avoid new dependencies if possible.
  - `tests/hub_daemon_lifecycle_test.rs`
  - `docs/client-protocol.md`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
- Reference-only production paths:
  - `crates/botster-hub-client/src/lib.rs`
  - `src/daemon_transport.rs`
  - `src/client_api.rs`
  - `src/runtime.rs`
  - `src/packages.rs`

## Risks

- A helper that reaches into hub internals would fail the ticket intent. Keep `botster-hub-test-support` downstream-shaped: isolated subprocess plus `botster-hub-client` requests.
- Duplicating the existing daemon test assertions inside both test-support and hub tests can create drift. Prefer one public helper with hub tests as a consumer.
- A pass based only on request dispatch would be too weak. The helper must inspect returned route descriptors, wrapped surface identity, UI body fields, action states, diagnostics, and configuration values.
- Error classification can become vague if all failures collapse into `UnexpectedKind`. Add or reuse precise errors for missing package, missing route, missing diagnostic, missing JSON field, and unexpected value.
- Environment failures can leak local paths through daemon stdout/stderr. Keep report structs path-neutral and use existing diagnostic summaries for startup/setup failures.
- Support matrix drift risk: if the new generic helper supersedes Project Pipelines as the plugin-surface conformance target, update `FirstPartyClientSupportMatrix.plugin_surfaces` and its stable JSON tests intentionally.

## Acceptance Checks And Tests

- Public test-support unit/doc coverage:
  - `./test.sh -p botster-hub-test-support`
  - Proves the new report struct serializes or compares stably if serialization is added, compile-checks usage examples, and verifies support matrix changes if any.
- Live isolated hub harness proof:
  - `./test.sh --test hub_daemon_lifecycle_test downstream_shaped_test_support_harness_starts_isolated_hub_and_reports_deterministic_conformance`
  - Or the renamed focused test that already exercises `IsolatedHubBuilder`; expected proof is that the real helper runs against a spawned hub and returns deterministic contract-matrix fields.
- Existing fixture path proof:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - This test may be renamed or refactored, but review must see that the real daemon path still installs/enables `fixtures/plugins/plugin-contract-matrix` and does not only call helper-local fake DTOs.
- Protocol/client crate gate if public DTO expectations or generated TypeScript are touched:
  - `./test.sh -p botster-hub-client`
- Runtime/user-path evidence required in review:
  - The helper starts or receives an `IsolatedHub` and talks through `botster-hub-client`.
  - The helper installs/enables the fixture package from a local path and verifies `ListPackages`, `ShowPackage`, route descriptors, `PluginSurfaceRender`, `PluginSurfaceAction`, and `SetPackageConfiguration`.
  - Blocked render and error action paths produce distinct diagnostics and leave the daemon responsive.
  - Docs include exact setup/build commands, explicit binary path requirements, and a minimal Rust test snippet for hub, web, TUI, and first-party plugin developers.
- PII/artifact check:
  - `rg -n "/U[s]ers/[^/]+|/h[o]me/[^/]+|BOTSTER_[A-Z_]*=.*(token|secret|key)|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+" docs/plans/expose-reusable-plugin-ui-conformance-harness-from-botster-hub-test-support.md docs/client-protocol.md fixtures/plugins/plugin-contract-matrix/README.md crates/botster-hub-test-support/src/lib.rs tests/hub_daemon_lifecycle_test.rs`
  - Expected: no introduced local home paths, emails, tokens, secrets, or raw personal identifiers.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/expose-reusable-plugin-ui-conformance-harness-from-botster-hub-test-support.md`.
- Plan gate should attach this artifact path plus the checklist timeout fallback evidence.
- Implement gate should report exact changed files, focused test commands, any skipped full-suite rationale, and whether support matrix semantics changed.
- Review should reject unwired helper code, request-dispatch-only evidence, private runtime internals in `botster-hub-test-support`, missing docs, or duplicate fake DTOs.

## Vault Gaps Worth Capturing

- Capture after implementation if `run_plugin_contract_matrix_conformance` becomes the generic successor to `run_project_pipelines_conformance` for plugin UI surface/action support-matrix evidence.
- Capture if a durable report/error taxonomy emerges for producer contract failure versus client rendering failure versus environment/setup failure.
- Capture if downstream clients settle on a specific JSON fixture export path for the plugin UI conformance report.
- No convention conflict found. The plan follows the hub-client external boundary, subprocess-spawned hub test-support convention, plugin-worker execution model, repo-visible plan artifact rule, and path-neutral vault citation rule.
