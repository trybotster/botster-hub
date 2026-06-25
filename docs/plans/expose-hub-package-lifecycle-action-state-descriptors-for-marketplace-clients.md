---
ticket: ticket_1782349956_572452
title: Expose hub package lifecycle action-state descriptors for marketplace clients
run: run_1782349987_143760
step: botster_plan
---

# Expose Hub Package Lifecycle Action-State Descriptors For Marketplace Clients

## Context loaded

- Pipeline context: ticket `ticket_1782349956_572452`, run `run_1782349987_143760`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, reviews, findings, questions, dependencies, or answers were present.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Required vault/project context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Checklist workflow: `project_pipelines_checklist_instructions` loaded. Standard run checklist `checklist_1782350063_972698` was created after an initial plugin worker timeout and should carry the vault evidence in parallel with this artifact.
- Repo context inspected: `src/packages.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, `tests/hub_daemon_lifecycle_test.rs`, and prior plan docs for marketplace registry sources, package lifecycle actions, package UI descriptors, and generated TypeScript protocol.
- Current production path: package rows are projected from `PackageRegistry` through `HubClientApi::ListPackages`, `HubClientPackage`, `daemon_package_from_client`, and `DaemonResponse.packages`. Available marketplace rows come from `PackageRegistry::available_packages`/`inspect_available_package` into `DaemonAvailablePackage`. Install/update/config/entrypoint actions already have public `DaemonRequest` variants and daemon transport handlers. The gap is a hub-owned, row-local action-state descriptor list that tells clients which lifecycle actions are available, blocked, or unavailable and which request they should dispatch.

## Scope

- Add public, generated daemon/client DTOs for package lifecycle action-state descriptors.
- Descriptors must include:
  - stable action id;
  - status, using an explicit vocabulary such as `available`, `blocked`, and `unavailable`;
  - blocked/unavailable reason and diagnostic rows when the hub denies or does not support an action;
  - required config/auth/dependency references where applicable;
  - request mapping for invokable actions, using existing daemon request type names and parameters where possible.
- Expose descriptors on installed `DaemonPackage` rows returned by list/show/package-decision/update/config flows.
- Expose descriptors on available `DaemonAvailablePackage` rows returned by list-available, inspect-available, and install preview flows, at minimum for install and unsupported actions that clients might otherwise infer.
- Cover lifecycle actions named by the ticket: install, enable, disable, remove, start, stop, restart, check update, preview update, apply update, configure, plus unsupported reload/restart-hub style actions as explicit unavailable descriptors.
- Keep hub/package policy authoritative. Clients render descriptors and dispatch hub-provided request mappings; they should not infer lifecycle policy from package state, availability reasons, or entrypoint process state.
- Reuse existing package availability, configuration, dependency, auth/config resolution, update diagnostics, entrypoint process snapshots, and daemon request vocabulary before adding new policy branches.
- Regenerate `crates/botster-hub-client/generated/daemon-protocol.ts` from Rust DTOs and extend drift/serde tests so TypeScript clients receive the new DTOs.

## Non-scope

- No browser/TUI marketplace policy, custom UI command inference, or client-side lifecycle resolver.
- No hosted marketplace, signing, payments, remote fetcher, package execution during preview/list, or automatic install/enable/start behavior.
- No fake reload or hub restart implementation. Unsupported reload/restart-hub actions should be represented as unavailable diagnostics only.
- No package registry rewrite, broad package-manager abstraction, or parallel action vocabulary outside the public daemon/client DTOs.
- No React SPA, TUI, Rails relay, Lua plugin workflow, or Project Pipelines UI changes unless docs need to clarify the public DTO contract.

## Assumptions and unknowns

- Assumption: descriptor `action_id` values should match current daemon request operation names where practical, for example `install_package_registry_entry`, `enable_package`, `start_package_entrypoint`, `preview_package_update`, and `set_package_configuration`. If product wants shorter labels such as `install` or `configure`, the mapping should still carry the concrete daemon request type.
- Assumption: entrypoint start/stop/restart descriptors are per runnable entrypoint, not package-global, because requests require `entrypoint_id`.
- Assumption: available marketplace rows should expose install as invokable when compatibility/policy admits it, while enable/disable/remove/start/stop/restart/update/config are unavailable or blocked until the package is installed.
- Assumption: existing availability reasons are diagnostic inputs, not a replacement for action descriptors.
- Assumption: update descriptors can derive from current `package_update_status` and should preserve the existing explicit unsupported diagnostics rather than making update look generally available.
- Unknown: exact shape of `request_mapping` in DTOs. Prefer a small tagged struct with `request_type` and typed string parameters over raw JSON blobs, unless existing generator constraints make a bounded JSON value simpler.
- Unknown: whether unsupported reload and restart-hub should be one descriptor each (`reload_package`, `restart_hub`) or grouped diagnostics. Prefer explicit descriptors if clients might show those actions.

## Botster layers touched

- Rust hub package policy/projection.
- Rust daemon transport package response projection.
- Public `botster-hub-client` daemon DTOs.
- Generated TypeScript daemon protocol.
- Rust daemon/client tests and protocol docs.

No Lua plugin, TUI, React SPA, Rails relay, cloud provider, or Project Pipelines plugin behavior is required.

## Affected surfaces/files

- `crates/botster-hub-client/src/lib.rs`
  - Add action-state DTO structs/enums and fields on `DaemonPackage`, `DaemonAvailablePackage`, and update/install/config response DTOs if needed.
  - Add serde tests for exact JSON field names, optionality, blocked/unavailable diagnostics, required references, and request mappings.
- `crates/botster-hub-client/src/typescript.rs`
  - Extend the Rust-owned generator for new action-state DTOs and request-mapping types.
- `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Regenerate checked artifact; drift tests must fail if omitted.
- `src/client_api.rs`
  - Add sanitized action-state projection beside `HubClientPackage::from_record` if the internal projection remains the best policy assembly point.
  - Reuse `HubClientPackageAvailability`, configuration views, dependency/feature availability, and runnable entrypoint metadata.
- `src/daemon_transport.rs`
  - Map internal action states into public `DaemonPackage`/`DaemonAvailablePackage` DTOs.
  - Ensure list/show/package-decision/config/update/install-preview flows all return rows with descriptors.
  - Attach entrypoint process snapshots before deriving or finalizing start/stop/restart descriptor status.
- `src/packages.rs`
  - Touch only if action-state computation needs small package-policy helpers. Avoid moving daemon request policy into the registry unless it is genuinely package-state logic.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add focused live-daemon coverage for installed lifecycle descriptors, available-package install descriptor, blocked config/auth/dependency cases, unsupported update/reload diagnostics, and entrypoint request mappings.
- `tests/hub_client_api_test.rs`
  - Add projection tests if action states are assembled in `client_api`.
- `docs/client-protocol.md`
  - Document that hub-owned action states are the authoritative marketplace/client lifecycle contract.

## Risks

- Underwiring risk: adding DTO structs without routing them through list/show/available/update/config responses fails the ticket. Tests must hit daemon request paths, not only constructors.
- Client-policy leak risk: if descriptors omit blocked/unavailable cases, marketplace clients will keep inferring policy from package state.
- Request mapping drift risk: descriptor action ids can diverge from actual `DaemonRequest` variants. Tests should assert invokable mappings round-trip to the production request names/parameters.
- Entrypoint state race risk: start/stop/restart availability depends on current supervisor snapshots. Projection must apply snapshots before action-state derivation or descriptors may be stale.
- Privacy risk: required references and diagnostics must name config/auth/dependency keys without leaking local package roots, data dirs, or secret values.
- Scope creep risk: reload/restart-hub descriptors are diagnostics, not a mandate to implement those actions.
- DTO drift risk: generated TypeScript optionality must match Rust serde defaults and skips.

## Acceptance checks/tests

- `cargo fmt`.
- `BOTSTER_ENV=test cargo test -p botster-hub-client`
  - Proves Rust serde DTOs, request mapping names, and generated TypeScript artifact coverage.
- Focused generated protocol drift test in `botster-hub-client`, including `DaemonPackage`/`DaemonAvailablePackage` action-state fields and action-state DTO definitions.
- `./test.sh --test hub_daemon_lifecycle_test <focused installed action-state test>`
  - Install local package, list/show it, and assert install/enable/disable/remove/configure/update descriptors match package state.
- `./test.sh --test hub_daemon_lifecycle_test <focused available package action-state test>`
  - List/inspect available package and assert install descriptor is available with an install request mapping; non-installed-only actions are blocked/unavailable.
- `./test.sh --test hub_daemon_lifecycle_test <focused blocked references test>`
  - Package with missing config/auth/dependency emits blocked descriptors with required references and diagnostics, without secret/path leakage.
- `./test.sh --test hub_daemon_lifecycle_test <focused entrypoint mapping test>`
  - Runnable entrypoint rows expose start/stop/restart descriptors mapped to `start_package_entrypoint`, `stop_package_entrypoint`, and `restart_package_entrypoint` with package name and entrypoint id.
- `./test.sh --test hub_daemon_lifecycle_test <focused unsupported diagnostics test>`
  - Unsupported reload/restart-hub/update cases return explicit unavailable descriptors/diagnostics and do not execute packages during preview/list.
- Production entry proof for implementation/review: name the live path from `DaemonRequest::ListPackages`, `ShowPackage`, `ListAvailablePackages`, `InspectAvailablePackage`, update/config requests, and entrypoint status through `src/daemon_transport.rs` into returned `DaemonResponse` rows containing descriptors.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan gate evidence should include context loaded, scope/non-scope, assumptions/unknowns, affected surfaces/files, risks, acceptance checks, and vault gaps.
- Implementation gate should prove generated TypeScript changed, real daemon responses include descriptors, and tests dispatch through existing daemon request mappings.
- Review should reject unwired DTOs, client-inferred policy, fake reload/restart success, raw JSON request blobs when typed mapping is practical, missing blocked references, missing generated artifact updates, and path/secret leakage.

## Worktree and target assumptions

- Assigned worktree: the ticket's assigned worktree.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must operate in this pipeline-created worktree, not an ambient checkout.

## Vault gaps worth capturing

- Capture the final action-state DTO vocabulary once implementation settles it: status enum, descriptor fields, required-reference shape, and request-mapping shape.
- Capture the rule that marketplace clients consume hub-owned lifecycle descriptors instead of inferring package policy from state fields.
- Capture the exact unsupported reload/restart-hub action ids and diagnostic kinds if they become stable.
- No convention conflict found. The plan follows hub-owned package policy, daemon-owned mutation, public hub-client DTO authority, generated TypeScript drift checks, explicit worktree/target binding, and no speculative marketplace/network implementation.
