# Add Hub Marketplace Registry Sources and Install Plan Preview

## Context Loaded

- Pipeline context: ticket `ticket_1782338822_376426`, run `run_1782338884_109158`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, or answers were present.
- Playbooks and skills: [[planner-playbook]], [[botster-planner-playbook]], `botster-customize-hub`.
- Vault/project constraints: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[botster package registry persists through hub state json]], [[botster package records persist trust compatibility and admitted capability lock metadata]], [[botster package daemon dto exposes sanitized package rows]], and [[botster hub client crate is the external client boundary]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` was loaded. Creating the standard vault checklist for this run timed out with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and should also be included in gate evidence.
- Repo context inspected: `src/packages.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/main.rs`, `src/persistence.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`, prior `docs/plans/*`, `Cargo.toml`, and `test.sh`.
- Current production path: package mutations and reads already flow through `DaemonRequest` in `botster-hub-client`, `src/daemon_transport.rs`, live `HubDaemon.package_registry`, `HubState.package_registry`, and sanitized `DaemonPackage` rows. The plan extends that path instead of adding a direct state-file writer or a separate registry service.

## Scope

- Add a hub-owned marketplace registry-source model for local/static registry fixtures:
  - local path registry sources;
  - first-party static registry entries;
  - git-shaped entry metadata carrying repo plus pinned branch/tag/rev fields;
  - source metadata that can be persisted with installed package records.
- Add available-package and inspect-before-install DTOs to the public daemon/client protocol. These should let clients list available packages from configured registry sources, inspect one registry entry, and see installed-vs-available state without exposing raw local paths or unchecked registry internals.
- Add an install-plan preview DTO and daemon request path that evaluates the selected registry entry against current hub policy before installation:
  - package identity/version/classification;
  - source kind and pin summary;
  - requested capabilities;
  - compatibility result and diagnostics;
  - installed/current state;
  - install effects such as "would add package record", "would update source metadata/pin", "would require explicit enable", and "would not start entrypoints".
- Implement explicit installation from an inspected registry entry only for local/first-party fixture entries, with no network fetch. Installation should persist source metadata and pins in `HubState.package_registry`.
- Preserve existing `packages install --path` and `packages enable --path` behavior as local-path convenience flows; do not replace them with the registry-source preview flow.
- Update generated TypeScript protocol so browser clients consume the authoritative Rust serde DTOs.
- Add focused fixtures/tests covering local path registry entries and git-shaped registry entries without network dependency.
- Update `docs/client-protocol.md` and CLI package output only enough to make list/inspect/preview/install paths understandable and path-neutral.

## Non-Scope

- No hosted marketplace, remote index service, ratings, payment, signing, trust delegation, dependency solving, or network clone/fetch.
- No automatic execution, auto-enable, or auto-start of package entrypoints after install.
- No package supervision changes.
- No plugin workflow policy changes and no Project Pipelines UI work.
- No broad refactor of `PackageRegistry`, daemon transport, CLI parsing, or generated protocol machinery beyond what is necessary for the new request/response DTOs.
- No raw `PackageRecord`, provenance paths, local package roots, or PII-bearing filesystem paths in public DTOs.

## Assumptions and Unknowns

- Assumption: "hub-owned marketplace registry source" means a local/static registry catalog owned by the hub profile, not a plugin-owned registry and not a hosted marketplace.
- Assumption: "git repo metadata shape" means registry and lock metadata only. It should persist repo and pin fields, but it must not clone, fetch, checkout, or install git content in this ticket.
- Assumption: core `PackageSource::Git` currently has `repo` and `reference`; hub can represent richer branch/tag/rev pins in hub-owned registry/source metadata without requiring a core dependency change unless implementation discovers an existing upstream shape.
- Assumption: install-plan preview should reuse existing manifest compatibility/capability admission helpers where possible, but should not store incompatible preview entries as installed records unless the ticket explicitly grows rejected available-package state.
- Assumption: installed-vs-available state can be computed by matching registry entry package name against `PackageRegistry.package(name)` and returned as a client DTO state such as `available`, `installed`, `enabled`, or `disabled`; persisted package state remains the existing `Installed`/`Enabled`/`Disabled`.
- Assumption: source metadata and pins belong on the hub-owned lock side, probably `PackageProvenance`, `PackagePin`, or a small new source-metadata struct on `PackageRecord`, not in core manifests.
- Unknown: exact CLI spelling. Prefer a minimal extension of existing `packages` commands, for example `packages available`, `packages inspect <entry>`, `packages preview-install <entry>`, and `packages install <entry>` or equivalent if local parser conventions point elsewhere.
- Unknown: registry fixture location. Prefer a repo-local deterministic fixture under `examples/` or `tests/fixtures` if the current test style supports it; avoid runtime home/config discovery in tests.

## Botster Layers Touched

- Rust hub package/registry policy.
- Rust daemon transport.
- Public `botster-hub-client` daemon protocol DTOs.
- Generated TypeScript daemon protocol artifact.
- Thin CLI package commands/output.
- Tests and protocol docs.

No Lua plugin, TUI rendering, React SPA component, Rails relay, or MCP workflow behavior is required.

## Affected Surfaces and Files

- `src/packages.rs`
  - Add registry-source/catalog structs and parsing for local/static fixture entries.
  - Add install preview/effect structs that evaluate manifest source, compatibility, capability requirements, existing package state, and explicit post-install enablement.
  - Extend installed records or existing provenance/pin structs to persist registry source metadata and branch/tag/rev pins.
  - Add unit tests for local path entries, git-shaped entries, installed-vs-available state, compatibility/capability preview, and snapshot persistence.
- `src/persistence.rs`
  - Prove `HubState.package_registry` persists source metadata and pins on installed records.
- `src/client_api.rs`
  - If the existing local client facade remains the internal projection point, add sanitized hub-client DTOs for available packages and install plans beside existing package projections.
- `src/daemon_transport.rs`
  - Add `DaemonRequest` handlers for list available, inspect registry entry, preview install, and explicit install from registry entry.
  - Ensure mutation handlers persist through `persist_package_registry(daemon)?` and return refreshed package/available state.
- `crates/botster-hub-client/src/lib.rs`
  - Add public serde request/response DTOs and response kind(s) for available registry entries, inspected entry, and install plan preview.
  - Add serde-stability tests for exact JSON field names and optionality.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Regenerate/check protocol types from Rust serde DTOs.
- `src/main.rs`
  - Add thin CLI parsing and path-neutral output for the new package registry/preview commands.
- `tests/hub_client_api_test.rs`
  - Cover DTO projection and path/provenance sanitization.
- `tests/hub_daemon_lifecycle_test.rs`
  - Cover the real daemon path: list available from fixture, inspect one entry, preview install effects, install explicitly, then verify persisted source metadata/pins after reload.
- `docs/client-protocol.md` and possibly `README.md`
  - Document the new daemon requests, preview semantics, no-network/no-execution boundary, and generated protocol authority.

## Risks

- Underwiring risk: adding structs without routing them through `DaemonRequest` would not satisfy acceptance. Tests must exercise the real daemon/client path, not only unit constructors.
- Network creep risk: git-shaped entries can be mistaken for clone support. The implementation must keep git support to metadata and preview/pin persistence only.
- Auto-execution risk: install flows already have enable/start adjacent commands. New install-from-registry must remain install-only and must not call enable, lifecycle load, or entrypoint supervisor start.
- DTO drift risk: hand-written TypeScript or docs can diverge from Rust serde. The generated protocol artifact and drift test must remain authoritative.
- Privacy risk: local registry paths, package roots, and provenance internals can leak through inspect/preview output. DTOs and CLI output should use sanitized source labels and test against path leakage.
- Compatibility-policy duplication risk: preview can accidentally implement a second admission engine. Prefer reusing `PackageCompatibility::for_manifest`, existing capability-grant checks, and `PackageRegistry` helpers or extracting small shared helpers from current install/enable logic.
- Snapshot compatibility risk: adding persisted source metadata fields must use serde defaults so existing `hub-state.json` records continue to load.

## Acceptance Checks and Tests

- Focused package unit tests through `./test.sh packages::` or exact discovered filters for:
  - local path registry entry parsing;
  - first-party/static registry entry classification;
  - git-shaped repo metadata with pinned branch/tag/rev fields;
  - installed-vs-available state computation;
  - preview diagnostics for compatibility and capabilities;
  - persistence of source metadata/pins through `PackageRegistrySnapshot`.
- Public protocol tests in `botster-hub-client`:
  - serde JSON for new request/response variants uses exact expected field names;
  - generated TypeScript includes the new DTOs and optional fields correctly;
  - request/response kind coverage guards include new variants.
- Daemon/client path tests:
  - list available packages from a first-party/local registry fixture;
  - inspect one entry;
  - preview install effects without mutating `HubState.package_registry`;
  - explicitly install the entry and verify package state remains installed/disabled unless the user separately enables it;
  - reload hub state and verify source metadata/pins persist;
  - prove no entrypoints are started by install.
- CLI smoke coverage if CLI commands are added:
  - `packages available`, inspect, preview, and install output is path-neutral and uses the daemon request path.
- Required commands after implementation:
  - `cargo fmt`
  - `./test.sh -p botster-hub-client`
  - targeted `./test.sh --test hub_client_api_test <focused test>`
  - targeted `./test.sh --test hub_daemon_lifecycle_test <focused test>`
  - full `./test.sh` if shared package/daemon protocol behavior changes broadly.
  - `cargo clippy --all-targets --all-features -- -D warnings` if touched code introduces nontrivial shared helpers or if the repo gate expects strict clippy for the final diff.
- Production entry proof: evidence must name the live path from CLI/public client request to `src/daemon_transport.rs`, `HubDaemon.package_registry`, `PackageRegistry` preview/install logic, persisted `HubState.package_registry`, and returned public `DaemonResponse` DTOs.

## Pipeline Gates and Artifacts

- Plan artifact: this file.
- Plan gate evidence should include the context loaded, checklist timeout fallback, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Implement gate should require committed code plus evidence that preview and install use the running daemon owner and that generated protocol stays in sync.
- Review/Verify should inspect for unwired DTOs, synthetic-only tests, accidental network access, raw path leakage, auto-enable/start behavior, and stale generated TypeScript.

## Worktree and Target Assumptions

- Assigned worktree: the pipeline-provided ticket worktree for `ticket_1782338822_376426`.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must operate in the assigned worktree and must not use ambient checkout paths.

## Vault Gaps Worth Capturing

- Capture after implementation whether Botster marketplace registry sources are a hub-owned local/static catalog contract and which struct owns source metadata.
- Capture the settled branch/tag/rev pin vocabulary for git-shaped registry entries.
- Capture whether install-plan preview should become the general package admission dry-run pattern for future install/update flows.
- No new vault note is needed yet for checklist timeout; this run matches the existing [[project pipelines checklist worker timeouts require artifact evidence fallback]] pattern.
