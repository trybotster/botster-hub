# Publish External Client Conformance Fixtures From Botster Hub

## Context Loaded

- Project Pipelines context: ticket `ticket_1781026822_590741`, run `run_1781026831_214075`, active step `botster_plan`, gate `botster_plan_gate`, no prior artifacts/findings/questions/answers.
- Vault/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[plan agents must author vault context as wikilinks not home paths]], and [[test script required for rust tests not cargo test]].
- Repo context inspected: `Cargo.toml`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `crates/botster-hub-test-support/Cargo.toml`, `tests/hub_daemon_lifecycle_test.rs`, `tests/support/mod.rs`, `docs/client-protocol.md`, `examples/project-pipelines/plugin.lua`, and `examples/project-pipelines/README.md`.
- Existing baseline: the repo already has the predecessor isolated hub harness in `botster-hub-test-support`, the authoritative client protocol in `botster-hub-client`, docs for downstream isolated daemon tests, and hub-owned daemon tests proving status/list/spawn/attach/drain/input/resize/teardown through real socket calls.
- Checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` was attempted for this run and timed out with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and gate evidence.

## Scope

- Publish a hub-owned conformance surface from `crates/botster-hub-test-support` that downstream first-party and third-party clients can import without depending on full `botster-hub`, TUI, Lua runtime, plugin internals, or private session-worker wire formats.
- Add deterministic conformance flow helpers or fixtures that execute through `botster-hub-client` against an isolated hub subprocess. The helpers should cover:
  - daemon status and synthetic hub identity;
  - session list on an empty isolated hub;
  - successful local session spawn;
  - attach plus drain of terminal output;
  - input echo through the client protocol;
  - resize where supported by the daemon request surface;
  - detach and session teardown;
  - plugin/entity/action dispatch through public daemon requests where the current hub surface already exposes them, especially `PluginSurfaceRender` and `PluginSurfaceAction`;
  - at least one validation/error path, such as unknown session attach/drain or invalid Project Pipelines create-ticket action.
- Keep `botster-hub-client` as the protocol source of truth. The conformance package may sequence public requests and assert public response/event shapes, but it must not mirror private wire frames or define a second DTO layer.
- Add hub-owned tests that prove the published conformance fixtures drive an isolated hub through real `botster-hub-client` calls.
- Update `docs/client-protocol.md` or a crate-level rustdoc section in `botster-hub-test-support` with exact downstream dependency/API usage, explicit binary path requirements, isolated data-dir/socket lifecycle, and synthetic device identity expectations.

## Non-Scope

- Do not modify `botster-tui` or `botster-web`.
- Do not add a spec-only document without runnable/importable fixtures.
- Do not make downstream clients depend on `botster-hub` internals, TUI code, plugin worker internals, `botster_core::contract` session frames, or daemon-to-session-worker protocol types.
- Do not add optional configurability, broad protocol abstraction, or a client compatibility matrix beyond the flows required by this ticket.
- Do not mutate real user identity, default Botster home state, or non-test data directories.

## Botster Layers Touched

- Rust hub test-support crate: primary implementation surface.
- External client protocol crate: import-only source of request/response/event truth; avoid semantic duplication.
- Hub daemon lifecycle and session/client-worker runtime: exercised only through subprocess and public client calls.
- Example Project Pipelines plugin: optional fixture for public plugin surface/action validation if the current package enablement path is sufficient.
- Docs: external client conformance usage and lifecycle.
- No SPA, Rails relay, TUI, or core protocol rewrite.

## Assumptions And Unknowns

- Assumption: the existing `botster-hub-test-support` crate is the correct publication boundary; this ticket extends it rather than creating another crate.
- Assumption: Unix-only support remains acceptable because the daemon socket and current tests are already Unix-gated.
- Assumption: downstream callers can provide explicit `botster-hub` and `botster-session-worker` binary paths, matching the current `IsolatedHubBuilder` API and avoiding Cargo-only `CARGO_BIN_EXE_*` assumptions.
- Unknown: how much plugin/entity/action coverage should be mandatory for a generic external client. Plan decision: include a public plugin surface/action flow if it can be done through existing package-enable and daemon request variants without adding plugin internals; otherwise document the unsupported subflow explicitly and keep the fixture limited to public daemon requests.
- Unknown: whether conformance output should be a single result struct or multiple small flow functions. Prefer small typed flow helpers with deterministic assertions and returned observations only where downstream CI needs diagnostics.
- Worktree/target assumption: implementation happens in the pipeline-assigned ticket worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.

## Affected Surfaces And Files

- `crates/botster-hub-test-support/src/lib.rs`: add public conformance fixtures/helpers, likely around the existing `IsolatedHub` and `IsolatedHubBuilder`.
- `crates/botster-hub-test-support/Cargo.toml`: update metadata only if needed; avoid new dependencies unless Implementation proves a standard-library solution is not enough.
- `tests/hub_daemon_lifecycle_test.rs`: add or refactor tests so the hub proves the published conformance fixture drives real subprocess/socket behavior through `botster-hub-client`.
- `docs/client-protocol.md`: document exact downstream dependency, API usage, lifecycle, deterministic output, and isolation guarantees.
- Potentially `examples/project-pipelines/README.md` or fixture docs only if the plugin action validation flow becomes part of the conformance fixture.

## Risks

- Protocol duplication: wrapping request/response types too deeply could create a second source of truth. Mitigation: helpers should call and expose `botster_hub_client` types where practical.
- Flaky subprocess tests: real daemon and PTY tests can race. Mitigation: reuse isolated data dirs, existing daemon test lock, bounded waits, and deterministic shell commands.
- Hidden host-state mutation: default daemon startup could touch real Botster state. Mitigation: every fixture starts from explicit data dir/socket and asserts test-local paths plus synthetic/default non-PII identity.
- Overfitting to current tests: fixtures that only satisfy hub tests may not help downstream clients. Mitigation: write the API and docs from the downstream dependency shape first, then prove the same API in hub tests.
- Plugin action ambiguity: entity/action dispatch currently crosses daemon requests as JSON for plugin surfaces/actions. Mitigation: include only public daemon calls and one validation path; do not reach into plugin worker state.
- Determinism drift: terminal output can include timing-dependent events. Mitigation: fixture assertions should inspect stable response kinds, session ids, output substrings, and action result fields rather than full event ordering.

## Acceptance Checks And Tests

- Add a hub-owned test that calls the published conformance fixture from `botster-hub-test-support`, passing explicit hub and session-worker binary paths, and verifies status/list/spawn/attach/drain/input/resize/detach/shutdown through real `botster-hub-client` calls.
- Add a validation/error test path through the same fixture, such as unknown session request returning a deterministic operator error, or invalid Project Pipelines action returning a failure result with stable field/form errors.
- Preserve or update the existing external proof tests:
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol`
  - any renamed/new conformance fixture test filter chosen by Implementation.
- Run `./test.sh -p botster-hub-test-support` if crate-local tests or rustdoc examples are added.
- Run the targeted daemon lifecycle test filter for the conformance flow through `./test.sh`, not raw `cargo test`, so `BOTSTER_ENV=test` is set.
- Run a doc or compile check for any public rustdoc example added to the test-support crate. If direct rustdoc testing needs Cargo rather than `./test.sh`, set `BOTSTER_ENV=test` and explain the exception.
- Verification report must name the production entry point changed: downstream test code imports `botster-hub-test-support`, starts an isolated hub subprocess, then the fixture exercises `botster_hub_client::request` or `DaemonConnection::request` over the daemon socket.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/publish-external-client-conformance-fixtures.md`.
- Gate evidence should include this plan path plus the checklist fallback evidence because checklist creation timed out.
- Plan Review should reject implementation plans that add private wire-format mirrors, depend on host-internal TUI/plugin code, or only document behavior without runnable fixtures.

## Vault Gaps Worth Capturing

- Capture after implementation if the resulting conformance fixture shape becomes durable guidance: external client conformance should live in hub-owned test-support crates that drive real subprocess hubs through public client protocol crates.
- Capture after implementation if a stable convention emerges for plugin/entity/action conformance flows over `PluginSurfaceRender` and `PluginSurfaceAction`.
- No new vault capture is needed at Plan time for the checklist timeout; [[project pipelines checklist worker timeouts require artifact evidence fallback]] already covers the fallback.

## Checklist Evidence Fallback

- Vault/context evidence: notes listed in `Context Loaded` constrained the plan to hub/client boundaries, explicit worktree/target assumptions, repo-visible artifacts, public protocol composition, path-neutral vault references, and `./test.sh` verification.
- Convention-conflict evidence: none found. The ticket aligns with [[botster hub client crate is the external client boundary]] and [[external client hub tests use subprocess spawned hub test support]] as surfaced through [[botster-architecture]] and [[cli-patterns]].
- Verification evidence gathered during planning: repo inspection confirmed the predecessor harness and docs exist; no implementation tests were run during Plan.
- Capture evidence: no durable knowledge was captured yet; capture is deferred until Implementation proves the final fixture API.
