---
ticket: ticket_1782338822_458421
title: Complete hub package lifecycle actions for marketplace v1
run: run_1782342640_529705
step: botster_plan
---

# Complete hub package lifecycle actions for marketplace v1

## Context loaded

- Pipeline context: ticket `ticket_1782338822_458421`, run `run_1782342640_529705`, current step `botster_plan`, gate `botster_plan_gate`; dependency `ticket_1782338822_376426` is closed and merged to `origin/main` as PR #71 / merge `08dc694`.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Required Botster/vault context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]].
- Re-plan context after Plan Review: [[stale project pipeline worktrees can miss merged dependency apis]], [[plan review must verify unmerged unregistered ticket dependencies]], [[plan steps need reviewable plan artifacts]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` loaded; run checklist `checklist_1782342699_606356` created for vault workflow evidence.
- Repo context inspected: `src/packages.rs`, `src/daemon_transport.rs`, `src/entrypoint_supervisor.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`, `README.md`, and prior plan docs for local package install/remove, runnable entrypoints, and entrypoint supervision.
- Current runtime shape after rebasing on PR #71: install/show/configure/enable/disable/remove, marketplace available/inspect/preview-install/install, and start/stop/restart/status entrypoint daemon requests already exist. Disable/remove already stop package entrypoints first. The merged install preview contract exposes `PreviewPackageInstall`, `DaemonPackageInstallPlan`, and `DaemonPackagePin`; this ticket must reuse that family for update preview/apply rather than defining parallel preview/pin DTOs. The remaining gap is check-update, preview-update, apply-update, and reload-required/restart-required diagnostics on the public daemon/client DTO surface.
- Plan Review outcome: `review_1782343241_343243` blocked this run on sequencing, not architecture. That blocker is now stale: `git fetch origin` advanced `origin/main` to `08dc694`, and this worktree branch was rebased to that commit. `git log --oneline -3 --decorate` now shows `08dc694 (HEAD -> project-pipelines/ticket_1782338822_458421, origin/main, origin/HEAD) Merge pull request #71...`.
- Re-plan decision: proceed from current `origin/main`, consume the merged `DaemonPackagePin`, `DaemonPackageInstallPlan`, and `PreviewPackageInstall` DTOs, and map this ticket's update check/preview/apply surface onto those contracts instead of creating a second preview/pin family.

## Scope

- Complete the package lifecycle action surface through public daemon/client DTOs and the thin CLI:
  - inspect/list/show existing package state;
  - install local package path as disabled;
  - enable and disable package policy/lifecycle;
  - remove package after stopping package entrypoints and unloading plugin lifecycle;
  - start, stop, restart, and status one runnable entrypoint;
  - check update;
  - preview update;
  - apply update to pinned source metadata;
  - report reload/restart requirements or structured unavailable diagnostics.
- Add explicit daemon request variants and generated DTO coverage for update lifecycle actions and lifecycle diagnostics. Keep additions additive and serde-defaulted where possible.
- Keep mutation ownership inside the running daemon. CLI commands must call `daemon_transport_request`; no offline package mutation path.
- Reuse `PackageRegistry`, `PackagePin`, `PackageUpdatePolicy`, package configuration, `PackageEntrypointSupervisor`, `DaemonPackageDecision`, `DaemonPackageDiagnostic`, and existing operator error frames before adding new types.
- Add a narrow registry update model:
  - check/preview returns a typed diagnostic result for unsupported source/update cases instead of pretending to fetch;
  - apply update records approved pinned source metadata on the installed package, preserving existing configuration values;
  - if a package is enabled or has live entrypoints, the response reports whether reload or restart is required instead of performing fake reload/hub restart behavior.
- Update CLI output and docs to surface update diagnostics, required restart/reload flags, and no-execute-until-enabled/started behavior.
- Regenerate `crates/botster-hub-client/generated/daemon-protocol.ts` from the authoritative Rust DTOs.

## Non-scope

- No hosted marketplace browser, remote package index, network fetcher, git clone/fetch/update, dependency solver, signature verification, sandbox, installer-managed binaries, or cloud/Rails/WebRTC/browser marketplace UI.
- No fake hub restart or plugin reload behavior. If reload/restart is unsupported, return structured diagnostics and required-action metadata.
- No broad package manager abstraction, service object, or separate persistence store. The durable package registry remains `HubState.package_registry`.
- No TUI, React SPA, Rails relay, Project Pipelines workflow policy, or Lua plugin UI changes unless documentation needs to describe the public client contract.
- No automatic execution on install or update. Installed packages remain non-running until explicitly enabled and runnable entrypoints remain `not_started` until explicitly started.

## Assumptions and unknowns

- Assumption: "inspect" maps to existing `packages list` and `packages show` plus public `DaemonPackage` rows. If a separate `InspectPackage` request is desired, that needs a human product decision before implementation.
- Assumption: "apply update to pinned source metadata" should use `PackageRegistry::pin`/`PackagePin` rather than replacing manifest/configuration state. The implementation should preserve package configuration and package identity while updating pin/checksum/update-policy metadata.
- Assumption: check/preview update can be production-shaped without network resolution by returning structured `unavailable` diagnostics for unsupported sources/resolvers. This satisfies the ticket clause that unsupported reload/update cases must be explicit.
- Assumption: applying update metadata is allowed only for an installed package with an explicit preview/update payload from a trusted caller; it must not imply package code was fetched or reloaded.
- Assumption: enabled package code reload remains unsupported unless the existing lifecycle adapter already has a real reload path for the package class. Do not invent reload semantics.
- Settled precondition: the dependency's registry-source/preview APIs are now on this run's `origin/main` base. Update lifecycle work should extend the merged preview/pin vocabulary rather than reintroducing a separate contract.
- Unknown: exact CLI verb spelling for update operations. Prefer compact thin commands such as `packages check-update <name>`, `packages preview-update <name>`, and `packages apply-update <name> --revision <rev> [--checksum <sum>] [--policy manual|track-source]` if the current parser shape supports them cleanly.

## Botster layers touched

- Rust hub package/registry policy and persistence.
- Rust daemon transport and running daemon owner path.
- Public same-device `botster-hub-client` daemon DTOs.
- Generated TypeScript daemon protocol.
- Thin `botster-hub packages` CLI.
- Rust daemon/client tests and docs.

No Lua plugin policy, TUI, React SPA, Rails relay, cloud provider, or Project Pipelines plugin surface should change.

## Affected surfaces/files

- `src/packages.rs`
  - Add package update/check/preview/apply decision helpers or narrow methods on `PackageRegistry`, reusing `PackageInstallPlan`/`PackagePin` semantics where update preview needs the same source, compatibility, capability, and effect vocabulary.
  - Preserve configuration values when applying pin/source metadata.
  - Add precise errors/diagnostics for unsupported update source, missing pin revision, unsupported reload, missing package, disabled package, and already-running entrypoint cases where current errors are too coarse.
- `src/daemon_transport.rs`
  - Add request handling for check update, preview update, and apply update, keeping response mapping aligned with the merged `daemon_package_install_plan`/`DaemonPackagePin` projection helpers.
  - Persist after apply update.
  - Return fresh package snapshots after every mutation.
  - Keep remove ordering as stop entrypoints, unload lifecycle, remove record, persist.
  - Attach restart/reload-required or unavailable diagnostics to response/package rows without restarting the hub.
- `crates/botster-hub-client/src/lib.rs`
  - Add public `DaemonRequest` variants and serde tests for update lifecycle actions.
  - Prefer extending/reusing `DaemonPackageInstallPlan`, `DaemonPackageInstallEffect`, `DaemonPackagePin`, and package diagnostics for update preview/apply. Add only the smallest update-specific DTO needed for restart/reload requirements or unavailable diagnostics.
  - Keep generated protocol compatibility additive.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Regenerate generated DTO coverage and update drift tests.
- `src/main.rs`
  - Add thin CLI parsing and deterministic output for update lifecycle actions.
  - Print required restart/reload/unavailable diagnostics path-neutrally.
- `src/client_api.rs`
  - Touch only if daemon package mapping needs new sanitized lifecycle/update fields.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add live daemon tests for no execution before enable/start, remove stopping entrypoints first, update preserving config and pin metadata, and explicit unsupported update/reload diagnostics.
- `tests/hub_client_api_test.rs`
  - Add or adjust tests if package DTO projection gains new lifecycle/update fields.
- `docs/client-protocol.md` and `README.md`
  - Document lifecycle actions, update diagnostics, reload/restart-required reporting, and local-only/no-fetch boundaries.

## Risks

- Underwiring risk: adding registry methods without daemon/client/CLI routing fails the ticket. Acceptance must prove the public daemon request path changed.
- Fake behavior risk: update/reload language can tempt placeholder success. Unsupported cases must be explicit `unavailable` diagnostics with no hidden reload or hub restart.
- Config-loss risk: applying update metadata could overwrite `PackageRecord` or manifest-derived fields and lose package configuration. Tests must set config before update and assert it survives.
- Process orphan risk: remove must continue to stop supervised entrypoints before deleting package state. Tests should verify the specific pid exits.
- DTO drift risk: Rust DTO additions must regenerate TypeScript and preserve serde defaults for omitted additive fields.
- Sanitization risk: diagnostics and CLI output must not leak local data dirs, package source roots, or secret configuration values.
- Contract drift risk: update lifecycle work could still fork the merged preview/pin DTO family. Mitigation: route update preview/apply through `DaemonPackageInstallPlan`/`DaemonPackagePin` semantics wherever possible and add only narrowly named update diagnostics when the install plan vocabulary is insufficient.
- Scope creep risk: marketplace v1 wording can expand into network package management. Keep this ticket to lifecycle action entrypoints and honest diagnostics over the local durable registry.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test cli_packages_local_path_install_enable_disable_remove_flow`
  - Existing install/show/enable/disable/remove runtime path stays green.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_stops_and_restarts`
  - Existing start/stop/restart action path stays green.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_cleans_up_on_disable_remove_and_shutdown`
  - Extend or pair with a remove-specific assertion that remove stops entrypoints before deleting package state and the pid exits.
- New focused daemon test: `package_lifecycle_does_not_execute_until_explicit_enable_and_start`
  - Install local package and prove plugin lifecycle is not loaded and runnable entrypoint state remains `not_started` until explicit enable/start.
- New focused daemon test: `package_update_apply_preserves_configuration_and_pin_metadata`
  - Install/configure package, apply pinned source metadata/update policy, restart daemon, and prove config plus pin/checksum/update policy survive.
- New focused daemon test: `package_update_unsupported_cases_return_structured_diagnostics`
  - Check/preview/apply unsupported update or reload cases and assert `unavailable` diagnostics, required action flags, and no fake reload/restart.
- `cargo test -p botster-hub-client`
  - Public request/response serde compatibility, request operation names, and generated DTO shape.
- `cargo test -p botster-hub-client generated_typescript_protocol_matches_checked_artifact`
  - Exact generated TypeScript protocol drift check.
- `cargo fmt`.
- `cargo clippy --all-targets --all-features -- -D warnings` if baseline allows; if not, Verify must attribute failures to touched vs untouched files.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan gate evidence should include context loaded, scope/non-scope, assumptions/unknowns, affected surfaces/files, risks, acceptance checks, and vault gaps.
- Implementation gate must prove the production user path changed: a running daemon accepts lifecycle/update requests through `botster-hub-client`, CLI output reflects the new diagnostics/state, generated TypeScript includes the DTOs, and tests show no installed package executes until enable/start.
- Review should reject unwired structs, fake reload/update success, missing config-preservation tests, missing generated DTO updates, or path/secret leakage.

## Worktree and target assumptions

- Assigned worktree: pipeline-created worktree for `ticket_1782338822_458421`.
- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must operate in the assigned worktree, not an ambient checkout.

## Vault gaps worth capturing

- Capture the final lifecycle update vocabulary: check update, preview update, apply update, and the exact structured unavailable diagnostic kinds.
- Capture whether `PackagePin` is the durable "pinned source metadata" contract for marketplace v1, or whether a separate source metadata record lands from the dependency.
- Capture the decided CLI spelling for package update lifecycle actions.
- Capture the rule that package update metadata changes may require reload/restart but must not perform fake reload/hub restart.
- No convention conflicts found. The plan follows hub-owned package policy, daemon-owned mutation, thin CLI, public hub-client DTOs, path-neutral artifacts, and no speculative marketplace/network implementation.
