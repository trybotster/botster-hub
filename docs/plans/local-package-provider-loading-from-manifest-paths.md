# Implement local package provider loading from manifest paths

## Context loaded

- Pipeline context: ticket `ticket_1780508732_975628`, run `run_1780517596_240880`, active Plan step `botster_plan`; no prior findings, reviews, artifacts, questions, or answers; dependency ticket "Define durable hub state model and storage boundary" is closed.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster planning constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Package/hub constraints: [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster hub is a first party host profile over core]], [[botster core host profile compatibility checks stay deliberately narrow]], and [[botster cloud should be an installable privileged provider not a hub dependency]].
- Verification/artifact constraints: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- Repo context inspected: `README.md`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `src/lib.rs`, `src/config.rs`, `src/packages.rs`, `src/persistence.rs`, `src/lifecycle.rs`, `src/runtime.rs`, `src/main.rs`, `tests/hub_plugin_lifecycle_test.rs`, and `tests/hub_runtime_test.rs`.
- Locked `botster-core` source inspected at the Cargo.lock revision. `PackageManifest` and `PackageSource::Path` are serde-enabled public core contracts; this repo should consume them directly.
- Plan Review context loaded after the first review returned `changes_required`: review `review_1780518129_765043`, findings `finding_1780518129_821211`, `finding_1780518129_829716`, `finding_1780518129_203757`, `finding_1780518129_138118`, and `finding_1780518129_385614`.

## Botster layers touched

- Rust hub policy layer: local package source resolution, registry persistence, and package admission.
- Rust hub runtime/lifecycle layer: prepare enabled local package records for `HubRuntime::load_plugin_package`.
- This ticket is intentionally a library plus integration-test scaffold. The accepted runtime proof is a test crossing local path install, durable registry reload, hub grant/admission enablement, local package preparation, and `HubRuntime::load_plugin_package` with a synthetic runtime bundle. A follow-up ticket should wire operator-facing binary or CLI commands into `src/main.rs` after the local package source model is proven.
- No SPA, Rails relay, TUI, MCP, cloud, marketplace, or plugin UI surface work.

## Scope

- Add a local package source loader that accepts either an explicit manifest file path or a package directory.
- For package directories, resolve a single conventional manifest filename. Prefer a JSON core-contract manifest to avoid adding TOML parsing unless the existing locked core exposes a parser that requires another format.
- Parse into `botster_core::PackageManifest`, validate required manifest fields through existing `PackageRegistry::install` and `PackageRegistry::enable`, and normalize `PackageSource::Path` to the canonical local package root.
- Reject unsafe local paths before registry mutation: empty paths, missing paths, paths that canonicalize outside the selected package root, manifest paths whose parent cannot be determined, and entrypoint paths that are absolute or traverse outside the package root.
- Record local source/provenance/pin/update metadata in durable package state under the hub data directory. Use a small JSON file owned by `PersistenceBucket::PackageState` rather than SQLite or a new database abstraction.
- Add public hub-facing APIs around the existing policy surface, likely on `PackageAdmissionPolicy`, for:
  - installing from a local manifest path;
  - installing from a local package directory;
  - loading/saving durable package records;
  - enabling/disabling records through the existing grant/admission policy.
- Add a narrow preparation type for enabled local packages that returns the package name, canonical package root, manifest entrypoints, and the selected entrypoint path resolved under the package root. This is the scaffold that future concrete Lua/process runtime wiring can consume.
- Prove the actual runtime path changed by exercising: local path install -> durable reload -> enable through hub grants -> prepare enabled package -> `HubRuntime::load_plugin_package` with a synthetic runtime bundle.
- Keep `src/main.rs` out of scope except README-aligned wording if needed. It remains a thin smoke binary until a follow-up operator-facing package install/load command consumes these APIs.

## Non-scope

- No network fetch, git clone/fetch, marketplace index, marketplace UX, package browsing, update resolver, or remote checksum verification.
- No new provider implementation, cloud implementation, Rails integration, browser shell, ActionCable/WebRTC transport, MCP server changes, or TUI/SPA surface changes.
- No new hub-specific manifest vocabulary that duplicates `botster-core::PackageManifest`.
- No broad lockfile/package-manager design beyond the minimal durable local package registry needed for this ticket.
- No persistent plugin runtime database; plugin-owned state remains separate from hub package registry state.

## Assumptions and unknowns

- Assumption: "manifest paths" means a file containing serialized `botster_core::PackageManifest`, and "package directories" means directories containing that manifest at a conventional filename chosen by this implementation.
- Assumption: JSON is the manifest wire format for this local implementation. The ticket requires validating `botster_core::PackageManifest`, does not mandate TOML, and the locked core manifest type already derives serde. Adding `serde_json` as a normal dependency is the smallest reversible implementation.
- Assumption: durable state can be a JSON file in the configured hub data directory for this scaffold. The closed durable-state dependency intentionally delivered the `PersistenceBucket` boundary, and this ticket provides the first concrete `PackageState` materialization under that boundary using `HubConfig.data_directory`.
- Assumption: local install provenance uses a local source string, not the existing "non-local" wording. Use a stable `local:<canonical-package-root>` value or equivalent local scheme derived from the canonical package root, update the `PackageProvenance.source` doc comment to admit local sources, and assert the recorded value in tests.
- Unknown: exact future package manifest filename. Candidate: `botster-package.json` or `botster.json`; implementer should choose one, document it in README, and cover package-dir lookup in tests.
- Unknown: whether local package provenance should store raw absolute canonical paths. For test fixtures and display surfaces, prefer path-neutral assertions and avoid committing local-user paths. Runtime state on the local machine may need absolute canonical paths to reload packages correctly.
- Unknown: whether `PackageRegistry` should derive serde directly or persist via dedicated snapshot structs. Prefer dedicated snapshot structs if direct derives would make private registry internals or future core type changes too sticky.

## Affected surfaces/files

- `Cargo.toml`: move or add `serde_json` to normal dependencies if JSON parsing/persistence is implemented in production code.
- `src/packages.rs`: local install APIs, source/path validation, local provenance-source semantics, durable snapshot structs, persistence load/save helpers, local prepared-package type, and focused unit tests.
- `src/persistence.rs`: name the package registry file/path helper under `PersistenceBucket::PackageState`, if keeping persistence mechanics out of `packages.rs` would clarify ownership without creating a generic storage abstraction.
- `src/lifecycle.rs`: likely unchanged unless preparation needs an exported helper that validates selected entrypoint paths before constructing `HubPluginRuntimeBundle`.
- `src/runtime.rs`: likely unchanged except optional convenience wiring; avoid mixing package persistence into PTY/session mechanics.
- `src/lib.rs`: re-export new public package loader/persistence/preparation types.
- `src/main.rs`: no command wiring in this ticket; binary/operator wiring is a follow-up after the local package source model and durable registry are proven.
- `README.md`: update scaffold exclusions and package registry policy to document local-only manifest loading, durable registry location/shape, and the explicit absence of marketplace/git fetching.
- `tests/hub_plugin_lifecycle_test.rs` or a new integration test: synthetic local fixture package flow through install, persist/reload, enable/disable, prepare, and runtime load.

## Risks

- Scope creep into a full package manager. Keep the implementation local-only and registry-backed.
- Path traversal and symlink bypasses. Use canonical paths and component checks instead of string prefix checks; validate entrypoint paths before runtime preparation.
- TOCTOU remains possible if a symlinked package directory or entrypoint target changes between install-time validation and load-time preparation. Re-validate entrypoints against the canonical package root at preparation time, not only during install.
- Over-coupling durable JSON to internal registry layout. Snapshot structs can keep persistence stable while `PackageRegistry` remains policy-oriented.
- Runtime proof becoming parser-only. Acceptance for this scaffold requires the integration test to cross `PackageAdmissionPolicy` and `HubRuntime::load_plugin_package`, not only parse a manifest. Operator-facing binary wiring is an explicit follow-up.
- Dependency drift. `botster-core` is git-main in `Cargo.toml` but locked by `Cargo.lock`; avoid lockfile churn unless the locked manifest API cannot satisfy the ticket.
- PII in fixtures/artifacts. Synthetic package names, paths under `target/`, and `example.invalid` URLs only.

## Acceptance checks/tests

- `./test.sh` passes.
- Add targeted package tests that prove:
  - explicit manifest path installs a local package and records `PackageSource::Path` plus provenance;
  - local install records `PackageProvenance.source` using the chosen local scheme and updates the doc comment away from "non-local only";
  - package directory lookup resolves the conventional manifest file;
  - durable registry save/load preserves manifest, provenance, pin, update policy, enabled/disabled state, and admitted provider metadata where applicable;
  - enable/disable still uses existing grant/admission policy;
  - unsafe manifest/entrypoint paths are rejected, including absolute entrypoints, `..` traversal, and symlinked package/entrypoint cases where the canonical target escapes the package root;
  - duplicate install and missing source/provenance errors remain typed.
- Add an integration test using synthetic fixture package files under `target/` that runs local install -> save -> load -> enable -> prepare -> `HubRuntime::load_plugin_package` with the existing fake runtime bundle.
- Do not require a `src/main.rs` command path for this ticket. The integration test above is the accepted runtime proof because the ticket delivers the local source model and core lifecycle preparation scaffold; a follow-up operator-facing binary/CLI wiring ticket should consume the proven APIs.
- Run strict lint verification if repo standards require it after code changes: inspect `Cargo.toml` for `[lints]`; if strict lints are added or present, run the corresponding `cargo clippy --all-targets --all-features -- -D warnings` command and attribute any baseline failures to touched or untouched files.
- Optional runtime smoke after implementation: `./test.sh hub_runtime_loads_and_invokes_enabled_plugin_package_through_core_worker` or a new test-name filter that executes the local package lifecycle path. Do not use filename-like filters that could run zero tests.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Plan gate evidence should cite this artifact plus the loaded vault notes and repo inspection summary.
- Implement gate should require committed code, passing targeted tests, `./test.sh` evidence or exact unrelated-failure attribution, and a clear note proving the runtime path crosses the new local package provider.
- Review/Verify should check that no marketplace/git/network UX was added and that unsafe local path handling has direct tests.

## Vault gaps worth capturing

- If implementation settles the local manifest filename and format, capture a project note so future package/provider work does not rediscover it.
- If implementation settles the durable package registry file location and snapshot shape, capture that as a Botster hub persistence convention.
- If path traversal validation exposes a reusable local package-source rule, capture it near the existing package/provenance notes.
