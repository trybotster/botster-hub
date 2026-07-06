# Publish reusable plugin contract matrix test assets from botster-hub-test-support

## Context loaded

- Pipeline context: ticket `ticket_1783308847_286503`, run `run_1783308859_150843`, step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, questions, or answers were present.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]], [[identity]], [[goals]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Checklist discipline: `project_pipelines_create_vault_checklist` timed out with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist-equivalent evidence is preserved here and should be copied into gate evidence.
- Repo evidence inspected:
  - `crates/botster-hub-test-support/src/lib.rs` already exposes `IsolatedHubBuilder`, `run_client_conformance`, `first_party_client_support_matrix`, late-attach JSON fixtures, and `run_plugin_contract_matrix_conformance`.
  - `fixtures/plugins/plugin-contract-matrix` is the current hub-owned source fixture, but the public helper still requires callers to pass a checkout-relative path.
  - `crates/botster-hub-client/generated/daemon-protocol.ts` is the checked generated daemon protocol artifact; `crates/botster-hub-client/src/lib.rs` already verifies it against `daemon_protocol_typescript()`.
  - `docs/client-protocol.md` documents the generated TypeScript path and the contract-matrix helper, but still tells downstream clients to provide a checkout path to `fixtures/plugins/plugin-contract-matrix`.
  - `tests/hub_daemon_lifecycle_test.rs` proves the contract matrix through a real isolated hub, but locates the fixture with `env!("CARGO_MANIFEST_DIR")/fixtures/...`.

## Scope

- Publish the plugin contract matrix fixture as declared `botster-hub-test-support` test assets so a standalone consumer can obtain the fixture without a sibling `../botster-hub` checkout.
- Keep `fixtures/plugins/plugin-contract-matrix` as the in-repo source of truth, but add a package-consumable copy or embedded asset set inside `crates/botster-hub-test-support`.
- Add a small public asset API in `botster-hub-test-support`, for example:
  - a descriptor naming the contract matrix package and relative files;
  - a `copy_plugin_contract_matrix_fixture(destination)` helper returning the copied package path; and
  - a `plugin_contract_matrix_fixture_path()` helper only if it points to crate-managed assets and not to the repo root.
- Add a public protocol artifact API so clients can obtain the authoritative daemon TypeScript artifact from dependencies, not sibling paths. Prefer a narrow wrapper around the hub-client generated artifact, for example `daemon_protocol_typescript_artifact()` returning the generated contents and stable relative artifact name.
- Update `run_plugin_contract_matrix_conformance` docs/examples so downstream consumers use the asset helper by default, with environment variables or explicit paths documented only as local overrides.
- Add hub tests proving:
  - exported fixture assets are present;
  - exported fixture file contents match `fixtures/plugins/plugin-contract-matrix`;
  - the conformance helper can run against a copied exported fixture through a real isolated hub; and
  - the exported protocol artifact contents match `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Update `docs/client-protocol.md` and `fixtures/plugins/plugin-contract-matrix/README.md` with the dependency/API contract, optional override variables, and no-`../botster-hub` consumer workflow.

## Non-scope

- No new daemon request/response types, protocol version bump, package manifest vocabulary, plugin runtime primitives, or browser/TUI renderer implementation.
- No downstream `botster-web` or `botster-tui` checkout edits in this ticket.
- No private hub runtime coupling in `botster-hub-test-support`; it should remain an external-client-shaped crate using `botster-hub-client` plus subprocess hubs.
- No broad rewrite of `run_plugin_contract_matrix_conformance`; the gap is asset publication and stable artifact access, not the conformance flow itself.
- No removal of local override support. Explicit fixture paths and env vars may remain for local development, but normal documented use should come from declared dependencies.

## Assumptions and unknowns

- Assumption: crate-managed fixture assets under `crates/botster-hub-test-support` are acceptable even if they duplicate the source fixture, provided tests prove byte-for-byte equality with `fixtures/plugins/plugin-contract-matrix`.
- Assumption: copying the fixture to a caller-owned temp directory is better than returning paths inside Cargo registry source directories, because package install flows may mutate or inspect package roots.
- Assumption: the daemon protocol artifact can stay owned by `botster-hub-client`; `botster-hub-test-support` can expose or document a convenience wrapper without becoming a second protocol source of truth.
- Assumption: no human question is needed because the ticket explicitly asks for test-support or an adjacent stable test-support artifact, and the current repo already has the intended crate.
- Unknown: whether Cargo packaging needs `include` metadata for fixture assets. Implementer should verify with crate-local tests and, if practical, `cargo package -p botster-hub-test-support --list --allow-dirty` or an equivalent non-publishing package-list check.
- Unknown: final public function names. Choose boring descriptive names and keep them additive.

## Affected surfaces/files

- Botster layers touched: Rust hub test-support crate, hub-client generated protocol artifact access, checked-in plugin fixture assets/docs, daemon lifecycle tests, protocol docs.
- Primary implementation files:
  - `crates/botster-hub-test-support/src/lib.rs`
  - `crates/botster-hub-test-support/Cargo.toml` if package include metadata is needed
  - new crate-managed fixture asset files under `crates/botster-hub-test-support`
  - `tests/hub_daemon_lifecycle_test.rs`
  - `docs/client-protocol.md`
  - `fixtures/plugins/plugin-contract-matrix/README.md`
- Possible narrow hub-client touch:
  - `crates/botster-hub-client/src/lib.rs` only if adding a checked-artifact contents API beside the existing generator API is cleaner than a test-support wrapper.
- Reference-only surfaces:
  - `crates/botster-hub-client/generated/daemon-protocol.ts`
  - `fixtures/plugins/plugin-contract-matrix/{botster-package.json,plugin.lua,README.md}`

## Risks

- A helper that still returns `env!("CARGO_MANIFEST_DIR")/../../fixtures/...` would preserve the sibling-checkout dependency and fail the ticket intent.
- Duplicated fixture assets can drift. Add exact content parity tests and keep the source-of-truth relationship documented.
- Returning a borrowed path inside the crate source can invite consumers to mutate installed package assets. Prefer copying to a caller-supplied temp directory for conformance runs.
- Protocol artifact access can accidentally create a second source of truth. Keep Rust serde and the checked generated artifact in `botster-hub-client` authoritative.
- Docs can regress by showing `../botster-hub` as the normal path. The normal path should be dependency API first, explicit env/path override second.
- Cargo package contents may omit new assets unless checked. Verify package inclusion before review.
- PII/path leakage risk in docs and test reports. Use relative paths, temp dirs, and `example.invalid` fixture values only.

## Acceptance checks/tests

- Format:
  - `cargo fmt`
- Test-support crate:
  - `./test.sh -p botster-hub-test-support`
  - Must prove asset descriptors, copy helper behavior, protocol artifact wrapper, stable serialization if added, and fixture/source parity where crate-local tests can access both.
- Hub live conformance:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
  - This should use the exported/copy helper rather than a direct repo-root fixture path, and must still install/enable the copied fixture through a real isolated hub.
- Generated protocol artifact:
  - `./test.sh -p botster-hub-client` if `botster-hub-client` API or generated artifact checks change.
  - Add or keep a test proving exported protocol contents equal `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Packaging proof:
  - Run a package-list or equivalent check showing `botster-hub-test-support` includes its fixture assets. If the repo wrapper cannot run this cleanly, record the exact blocker and compensate with tests that compile from crate-managed assets.
- Docs/PII check:
  - `rg -n "../botster-hub|/U[s]ers/[^/]+|/h[o]me/[^/]+|BOTSTER_[A-Z_]*=.*(token|secret|key)|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+" docs/client-protocol.md fixtures/plugins/plugin-contract-matrix/README.md crates/botster-hub-test-support/src/lib.rs`
  - Expected: no introduced sibling-checkout normal path, local home paths, tokens, secrets, or email addresses. Any remaining `../botster-hub` mention must be explicitly described as obsolete or override-only.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/publish-reusable-plugin-contract-matrix-test-assets-from-botster-hub-test-support.md`.
- Plan gate should attach this artifact path plus checklist timeout fallback evidence.
- Implement gate should report exact changed files, public API names, fixture asset inclusion proof, focused test commands, docs updates, and whether `botster-hub-client` was touched.
- Review should reject unwired asset helpers, direct sibling checkout lookup as the default path, missing parity tests, omitted package-inclusion evidence, private hub internals in test-support, or docs that still require `../botster-hub` for normal client tests.

## Vault gaps worth capturing

- Capture after implementation if a durable convention emerges: reusable cross-repo fixtures should be copied from crate-managed test-support assets, while repo-root fixtures remain source-of-truth fixtures with parity tests.
- Capture if the final protocol artifact API becomes the preferred pattern for generated client artifacts from `botster-hub-client`.
- No convention conflict found. The plan follows the hub-client external boundary, subprocess-spawned hub test-support convention, minimal dependency preference, repo-visible plan artifact rule, and Project Pipelines checklist timeout fallback rule.
