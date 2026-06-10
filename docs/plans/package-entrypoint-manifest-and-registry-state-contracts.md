---
ticket: ticket_1781065269_190384
title: Define package entrypoint manifest and registry state contracts
run: run_1781065293_995022
step: botster_plan
---

# Define package entrypoint manifest and registry state contracts

## Context loaded

- Pipeline context: ticket `ticket_1781065269_190384`, run `run_1781065293_995022`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, or answers.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster architecture/vault constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Project pipeline checklist discipline: `project_pipelines_checklist_instructions` loaded. `project_pipelines_create_vault_checklist` was attempted for this run and failed with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and should be copied into gate evidence.
- Repo context: current worktree is clean. `src/packages.rs` owns local `botster-package.json` install parsing, local path/entrypoint validation, `PackageRecord`, `PackageRegistrySnapshot`, and prepare-time revalidation. `src/persistence.rs` persists `HubState.package_registry`. `src/client_api.rs` maps `PackageRecord` to sanitized `HubClientPackage`. `crates/botster-hub-client/src/lib.rs` owns public daemon DTOs including `DaemonPackage` and diagnostics. `src/daemon_transport.rs` maps hub-client packages to daemon DTOs and routes package mutations through the running daemon owner. `src/main.rs` prints package rows. `docs/client-protocol.md` and `README.md` document public client/package contracts.
- Upstream core manifest context: `botster_core::PackageManifest` currently has `entrypoints: Vec<ExtensionEntrypoint>`, and `ExtensionEntrypoint` is only `runtime`, `path`, `bootstrap`. The current hub uses this field as a code/plugin load entrypoint (`src/lifecycle.rs`, `src/lua_runtime.rs`), not as a runnable local process contract.

## Scope

- Add a hub-owned runnable package entrypoint contract for local/dev package entrypoints without changing process spawning behavior.
- Parse the new contract from `botster-package.json` during local package install while preserving the existing core `entrypoints` field for plugin/provider code loading. The contract must explicitly declare the ticket-required fields: stable id, kind, command, args, working-directory policy, environment requirements, mode (`dev`/`local`), capability needs, and `may_supervise`.
- Persist runnable entrypoint declarations in `PackageRecord`/`PackageRegistrySnapshot` so `hub-state.json` reload preserves the contract.
- Validate unsafe runnable-entrypoint paths and commands at install/reload boundaries:
  - duplicate entrypoint ids;
  - unsupported kinds;
  - missing command;
  - absolute or traversing command paths when command is package-relative;
  - unsafe/traversing working-directory values when a package-relative working directory is requested.
- Add stable process-state DTOs to the hub/client surface, but as state contracts only: `not_started`, `starting`, `running`, `exited`, `failed`, `stopped`, plus diagnostics. These DTOs should be available on package/entrypoint rows, initialized to `not_started` because this ticket does not spawn processes.
- Parse, persist, and expose `may_supervise` as a static manifest policy declaration while deferring actual supervision enforcement and process lifecycle transitions.
- Expose runnable entrypoints through sanitized client/daemon DTOs (`HubClientPackage` and `DaemonPackage`) so `packages list/show` and downstream clients can discover them without seeing raw local source paths.
- Document deferred production concerns: signing, sandboxing, dependency solving, installer-managed binaries, hosted marketplace, and production WebRTC.
- Add a local web client entrypoint example shaped for `botster-web`.

## Non-scope

- No process spawning, supervision, restart policy, PTY/session creation, health checks, or lifecycle transitions beyond static DTO state.
- No client repo edits, including no `botster-web` code changes.
- No marketplace fetching, package dependency solving, installer-managed binary resolution, signing enforcement, sandboxing, hosted marketplace, or production WebRTC.
- No broad package-system refactor and no migration away from the current daemon-owned package mutation path.
- No change to plugin lifecycle loading semantics that use core `entrypoints`.

## Assumptions and unknowns

- Assumption: because core already owns `entrypoints` as code-load entrypoints, the smallest compatible hub change is an adjacent hub-owned manifest extension such as `runnable_entrypoints` rather than overloading core `entrypoints`. If Plan Review wants the public key to be exactly `entrypoints`, that requires a larger core contract change or an explicit manifest compatibility decision.
- Assumption: the MVP should keep runnable commands local/dev only and represent mode as an enum with `dev` and `local`.
- Assumption: entrypoint kinds should use the ticket vocabulary unless implementation finds an existing local enum to reuse: `client`, `web`, `mcp`, `daemon`, `provider`.
- Assumption: command validation should allow either a bare command name found by future PATH/binary policy or a relative package path, but should reject empty, absolute, or traversing relative paths. Since spawning is deferred, validation proves syntax and package-boundary safety, not executable availability on the host PATH.
- Assumption: this repo has no separate package lockfile. Ticket language about "persisted registry/lock state" and acceptance language about "lock serialization" map to `PackageRegistrySnapshot` persisted inside `HubState.package_registry` in `hub-state.json`. If a separate lockfile is required, that should be a human question before implementation.
- Unknown: whether working-directory policy should be modeled as an enum/object (`package_root`, `entrypoint_dir`, `relative`) or string values. Prefer a typed serde enum with a relative-path variant to make unsafe path validation explicit.
- Unknown: exact environment-requirement shape. Prefer a declaration-oriented DTO such as required variable names plus optional defaults/descriptions, not resolved secret values or host environment snapshots.
- Unknown: whether capability needs should reuse `botster_core::Capability` directly or use a narrower DTO mirror. Prefer reusing `Capability` inside hub state to avoid duplicate surface vocabulary.

## Botster layers touched

- Rust hub package/registry layer.
- Rust hub persistence layer.
- Rust local client API and daemon socket DTO layer.
- Thin CLI output for package discovery.
- Docs/examples.

No Lua plugin policy, Project Pipelines workflow policy, TUI UI, React SPA, Rails relay, or MCP tool behavior is required.

## Affected surfaces/files

- `src/packages.rs`
  - Define `PackageRunnableEntrypoint` with stable id, kind, command, args (`Vec<String>`), working-directory policy, environment requirements, mode, capability needs, `may_supervise`, and static process-state/diagnostic types.
  - Add `runnable_entrypoints` to `PackageRecord` with serde defaults for backward compatibility.
  - Parse and validate runnable entrypoints from local manifest JSON, likely with a hub-owned wrapper that flattens/deserializes the core `PackageManifest` and reads the extra field.
  - Revalidate persisted runnable entrypoints in `PackageRegistry::from_snapshot`.
  - Add focused unit tests for parsing, duplicate ids, unsupported kinds, missing command, unsafe command/working-directory paths, persistence, and reload validation.
- `src/persistence.rs`
  - Extend persistence tests to prove `HubState.package_registry` serializes and reloads runnable entrypoints.
- `src/client_api.rs`
  - Add sanitized runnable-entrypoint/process-state DTOs to `HubClientPackage`, including args, environment requirements, capability needs, and `may_supervise`.
  - Map `PackageRecord` runnable entrypoints into client DTOs with no raw local package root leakage and no resolved environment values.
- `crates/botster-hub-client/src/lib.rs`
  - Add public serde DTOs for package runnable entrypoints, environment requirements, `may_supervise`, and process state diagnostics.
  - Add/update serde compatibility tests and fixture JSON as needed.
- `src/daemon_transport.rs`
  - Map `HubClientPackage` runnable entrypoints into `DaemonPackage`.
- `src/main.rs`
  - Print package entrypoint counts or compact entrypoint rows for `packages list/show` if existing CLI output expects package discovery to be human-visible. Keep output path-neutral.
- `tests/hub_client_api_test.rs`
  - Assert client package rows expose runnable entrypoints and initial `not_started` process state.
- `tests/hub_daemon_lifecycle_test.rs`
  - Assert real `botster-hub packages enable/list/show --data-dir` through a running daemon exposes persisted runnable entrypoints after restart.
- `examples/project-pipelines/botster-package.json` or a new package fixture
  - Add a local web client runnable entrypoint example shaped for `botster-web` while preserving existing Lua plugin `entrypoints`.
- `README.md` and/or `docs/client-protocol.md`
  - Document manifest shape, DTO process states, no-spawn MVP boundary, and deferred production concerns.

## Risks

- Naming risk: adding `runnable_entrypoints` may diverge from the ticket's phrase "package entrypoints", but overloading core `entrypoints` would break an existing production meaning. The plan makes this explicit for review.
- Underwiring risk: adding structs only in `src/packages.rs` would not satisfy the ticket. Acceptance must prove the running daemon package path exposes the new DTOs through `botster-hub-client`/CLI.
- Compatibility risk: persisted `hub-state.json` snapshots without runnable entrypoints must still deserialize via serde defaults.
- Path-leak risk: local package roots and source paths must not appear in client DTOs or package CLI output.
- Validation ambiguity: "missing commands" cannot mean host PATH lookup in a no-spawn ticket. It should mean absent/empty command field and unsafe package-relative command path, with executable resolution deferred.
- Environment risk: manifest environment requirements must stay declarative and sanitized; do not persist or expose host-resolved secret values.
- Scope creep risk: process state DTOs and `may_supervise` may tempt implementation of real supervision. Keep `may_supervise` as a parsed/persisted/exposed policy field and keep process state static/defaulted until a future spawning ticket.

## Acceptance checks/tests

- `./test.sh packages::` or equivalent focused package unit test filters covering:
  - manifest parsing for runnable entrypoints, including command, args, working-directory policy, environment requirements, mode, capability needs, and `may_supervise`;
  - duplicate entrypoint ids;
  - unsupported kinds;
  - missing command;
  - absolute/traversing command and working-directory paths;
  - registry snapshot serialization/reload preserving runnable entrypoints, including args, environment requirements, capability needs, `may_supervise`, and static process state.
- `./test.sh package_and_lifecycle_queries_are_sanitized_and_explicitly_pulled` updated or paired with a new `HubClientApi` test proving `HubClientPackage` exposes runnable entrypoints, args, environment requirements, capability needs, `may_supervise`, and `not_started` process state.
- `./test.sh --test hub_daemon_lifecycle_test cli_packages_enable_local_path_routes_through_running_daemon_and_persists` updated or paired with a focused daemon lifecycle test proving real CLI/daemon package list/show exposes the persisted entrypoint DTO after restart without leaking local paths or environment values.
- `cargo test -p botster-hub-client` for public DTO serde/default compatibility.
- `cargo fmt`.
- `cargo clippy --all-targets --all-features -- -D warnings` if the repo's current lint policy accepts it; if baseline failures exist, Verify must attribute diagnostics to touched vs untouched files.
- Manual/documentation check: README/client protocol includes the local `botster-web`-shaped web entrypoint example and deferred production concerns.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Gate evidence should include the context loaded, checklist timeout fallback, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Implementation gate should require committed code and evidence that the runtime user path changed: daemon-owned package install/list/show over `botster-hub-client` exposes the new entrypoint/process-state DTOs.

## Worktree and target assumptions

- Assigned worktree: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1781065269_190384`.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must operate in the assigned worktree, not an ambient checkout.

## Vault gaps worth capturing

- Capture after implementation whether Botster hub manifests should use `runnable_entrypoints` as the durable local/dev process contract while core `entrypoints` remains the plugin/provider code-load contract.
- Capture the exact working-directory policy vocabulary once implemented.
- Capture the exact environment-requirement vocabulary once implemented.
- Capture that package "lock serialization" is represented by `PackageRegistrySnapshot` in `hub-state.json`, unless implementation discovers or adds a distinct lock artifact.
- Capture the Project Pipelines checklist worker timeout only if it recurs beyond this known fallback pattern; this run hit `plugin worker invoke timeout` during checklist creation.
