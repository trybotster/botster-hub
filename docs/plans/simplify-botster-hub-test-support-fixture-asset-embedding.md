# Simplify botster-hub-test-support fixture asset embedding

## Context loaded

- Pipeline context: ticket `ticket_1783310723_590776`, run `run_1783310737_244381`, step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, reviews, findings, questions, or answers were present.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]], [[identity]], [[goals]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Specific constraints: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[external client hub tests use subprocess spawned hub test support]], [[botster first party client support matrices belong in hub test support]], [[published capability matrices must derive enumerations from source]].
- Repo evidence inspected:
  - `crates/botster-hub-test-support/src/lib.rs` currently declares `PLUGIN_CONTRACT_MATRIX_FIXTURE_FILES: &[(&str, &[u8])]` and then builds `PLUGIN_CONTRACT_MATRIX_FIXTURE_ASSET_FILES: &[TestAssetFile]` by manually indexing `[0]`, `[1]`, and `[2]`.
  - `plugin_contract_matrix_fixture_asset()` returns the `PluginContractMatrixFixtureAsset` consumed by `copy_plugin_contract_matrix_fixture()`.
  - Existing tests already prove asset descriptors, copy-helper writes, repo-source parity, and generated protocol artifact parity.
  - `crates/botster-hub-test-support/Cargo.toml` already includes `fixtures/plugin-contract-matrix/**` for packaging.
- Checklist discipline:
  - `project_pipelines_create_vault_checklist` initially returned `plugin worker invoke timeout`, then left checklist `checklist_1783310823_204241` behind.
  - The plan should update that checklist when possible and preserve equivalent vault/check evidence in gate evidence.

## Scope

- Simplify the plugin contract matrix fixture asset declarations in `botster-hub-test-support` to one source of embedded fixture asset metadata.
- Remove the redundant tuple metadata const for the contract matrix fixture asset set.
- Remove manual `[0]`/`[1]`/`[2]` indexing when constructing `TestAssetFile` values.
- Keep the public API shape unchanged unless the implementation needs a strictly internal rename:
  - `TestAssetFile`
  - `PluginContractMatrixFixtureAsset`
  - `plugin_contract_matrix_fixture_asset()`
  - `copy_plugin_contract_matrix_fixture(...)`
- Preserve whole-tree parity and copy-helper behavior exactly: the crate-managed embedded assets must still match `fixtures/plugins/plugin-contract-matrix`, and copied files must still land under `fixtures/plugin-contract-matrix`.
- Botster layer touched: Rust hub test-support crate only.
- Runtime/user path to prove: downstream tests call `copy_plugin_contract_matrix_fixture(...)`, which iterates `plugin_contract_matrix_fixture_asset().files`; that returned file slice must come from the simplified single asset source.

## Non-scope

- No new public fixture API, package manifest vocabulary, daemon protocol changes, hub runtime changes, plugin runtime changes, SPA/TUI changes, or docs rewrite.
- No dependency additions.
- No broad refactor of support matrix, daemon protocol artifact helpers, live isolated hub harnesses, or conformance flows.
- No edits to the fixture file contents unless a parity test proves they were already inconsistent and the implementer records that separately.

## Assumptions and unknowns

- Assumption: the ticket refers only to the contract matrix fixture asset embedding in `crates/botster-hub-test-support/src/lib.rs`, because that is the only located redundant tuple plus indexed `TestAssetFile` array.
- Assumption: preserving the public API is preferred because downstream clients may consume `plugin_contract_matrix_fixture_asset().files`.
- Assumption: a single `static` or `const` slice of `TestAssetFile` with each file's `include_bytes!` inline satisfies "one source of embedded fixture asset metadata."
- Unknown: whether the implementer will choose `static` or `const`; either is acceptable if it compiles, remains borrowed as `&'static [TestAssetFile]`, and avoids duplicate metadata.
- No human question is needed; the ticket is narrow and the repo evidence identifies the target unambiguously.

## Affected surfaces/files

- Primary implementation file:
  - `crates/botster-hub-test-support/src/lib.rs`
- Verification-only/reference files:
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/README.md`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/botster-package.json`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/plugin.lua`
  - `fixtures/plugins/plugin-contract-matrix/**`
  - `crates/botster-hub-test-support/Cargo.toml`
- Plan artifact:
  - `docs/plans/simplify-botster-hub-test-support-fixture-asset-embedding.md`

## Risks

- Accidentally keeping two parallel fixture lists under different names would fail the ticket intent even if tests pass.
- Replacing indexing with a helper that still consumes tuple metadata would reduce fragility but not reach "one source of embedded fixture asset metadata."
- Changing `relative_path` values can break downstream copied package layout.
- Dropping or reordering files can break existing descriptor tests and source-tree parity.
- Using generated or dynamic filesystem discovery for embedded assets would fight the current packaging model; keep compile-time embedded bytes.
- This is a cleanup-only ticket, so broad adjacent refactors would add review risk without acceptance value.

## Acceptance checks/tests

- Static/source checks:
  - `rg -n "PLUGIN_CONTRACT_MATRIX_FIXTURE_FILES|\\[[0-9]+\\]" crates/botster-hub-test-support/src/lib.rs`
  - Expected: no `PLUGIN_CONTRACT_MATRIX_FIXTURE_FILES` const remains, and no manual numeric indexing remains in the contract matrix fixture asset declaration.
- Focused tests:
  - `./test.sh -p botster-hub-test-support plugin_contract_matrix_fixture_asset_describes_published_files copy_plugin_contract_matrix_fixture_writes_caller_owned_package published_plugin_contract_matrix_fixture_matches_repo_source_tree`
  - These prove descriptor content, copy-helper behavior, and whole-tree parity after the simplification.
- Broader crate check if the focused filter is unsupported or too brittle:
  - `./test.sh -p botster-hub-test-support`
- Formatting:
  - `cargo fmt`
- PII/dependency check:
  - Confirm no new dependency appears in `Cargo.toml` or `Cargo.lock`.
  - `rg -n "/U[s]ers/[^/]+|/h[o]me/[^/]+|BOTSTER_[A-Z_]*=.*(token|secret|key)|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+" docs/plans/simplify-botster-hub-test-support-fixture-asset-embedding.md crates/botster-hub-test-support/src/lib.rs`
  - Expected: no local home paths, tokens, secrets, keys, or email addresses introduced.

## Pipeline gates and artifacts

- Plan gate should attach this plan artifact and checklist evidence.
- Implement gate should report:
  - exact changed files;
  - the final single-source asset declaration shape;
  - evidence that `copy_plugin_contract_matrix_fixture(...)` still uses `plugin_contract_matrix_fixture_asset().files`;
  - focused test command results;
  - confirmation that no dependency was added.
- Review should reject:
  - any remaining parallel contract matrix fixture asset arrays;
  - any remaining manual tuple indexing for that asset set;
  - unwired helper code that does not feed the existing public copy path;
  - missing parity/copy-helper test evidence;
  - unrelated fixture/API refactors.

## Vault gaps worth capturing

- No new durable vault gap is known at plan time. The existing notes already cover the relevant conventions: test-support ownership, source-derived/drift-guarded published artifacts, checklist fallback evidence, and repo-visible plan artifacts.
- Capture after implementation only if a sharper convention emerges, for example: "embedded cross-repo fixture asset lists should be a single typed `TestAssetFile` source, with repo-source parity tests guarding duplicated fixture bytes."
