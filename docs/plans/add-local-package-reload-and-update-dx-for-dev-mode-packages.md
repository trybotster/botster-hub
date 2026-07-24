# Add local package reload and update DX for dev-mode packages

## Context loaded

- Project Pipelines context: ticket `ticket_1782761720_817254`, run `run_1782772580_535804`, step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, reviews, open questions, or question answers. One closed dependency: "Add persistent local runtime bootstrap for local first-party packages".
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster vault context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected: `src/packages.rs`, `src/daemon_transport.rs`, `src/entrypoint_supervisor.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, and prior local runtime/app/package plans and reports.
- Checklist discipline: `project_pipelines_create_vault_checklist` was attempted for this run and timed out in the Project Pipelines plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence carry notes read, convention result, verification plan, and capture decision.

## Current repo shape

- Local package mutation is daemon-owned through `DaemonRequest::{InstallPackageLocalPath, EnablePackageLocalPath, EnablePackage, DisablePackage, RemovePackage}` in `src/daemon_transport.rs`.
- Local package install parses `botster-package.json` through `PackageRegistry::install_local_path` in `src/packages.rs`, validates manifest, runnable entrypoints, and session templates, then persists through the hub state snapshot.
- Enabled local Lua packages are loaded through `load_package_after_enable`, and package lifecycle reload already exists in `src/lifecycle.rs`, but there is no daemon/CLI reload request that re-reads an installed local package path.
- Runnable app state is exposed by `ListApps`, projected from package `runnable_entrypoints` plus `EntrypointSupervisor` snapshots. `apps open` starts app entrypoints through `StartPackageEntrypoint`.
- The local runtime bootstrap path from the closed dependency enables first-party local packages and starts `botster-web`, but currently re-running bootstrap does not provide a targeted "I edited this package, reload it" user path.

## Botster layers touched

- Rust hub package registry/policy: refresh installed local package records from their persisted source path.
- Daemon protocol and transport: add a reload request/response path through the live daemon owner.
- CLI: add a thin `packages reload <name>` command and local runtime-facing follow-up text if useful.
- Entrypoint supervisor: stop/restart only affected package entrypoints when the reload changes a package with running app processes.
- Generated/client DTO surface: update `botster-hub-client` request/action names and generated TypeScript protocol if the public daemon request enum changes.
- Tests/docs: focused daemon lifecycle tests plus README or command usage text.

## Scope

- Add an explicit local package reload action for packages originally installed from a local path.
- Re-read the package manifest/config/surface metadata/runnable entrypoint declarations from the same installed local path. Preserve package identity, enabled/disabled state, configuration values, trust/provenance shape, and admitted policy state where still valid.
- If the package is enabled, reload the Lua package lifecycle through the existing hub/core lifecycle path after the registry record is refreshed.
- If any entrypoint for that package is currently running, restart those affected entrypoints after the refreshed record is persisted and admitted. The restart must use the refreshed runnable entrypoint declaration and existing launch environment helper.
- Return clear diagnostics for reload blockers: package not installed, package not local-path backed, manifest parse/validation failure, package-name mismatch, compatibility/capability/configuration failure, lifecycle reload failure, build/output/readiness failure, and entrypoint restart failure.
- Keep diagnostics sanitized: package names, action names, diagnostic kinds, and bounded messages are fine; raw local package paths should not be printed in normal status output.
- Add CLI affordance: `botster-hub packages reload --data-dir <dir> <package-name>` or equivalent parser shape consistent with existing `packages` commands.
- Add or adjust package/app action descriptors so `packages show/list`, `apps list`, and `apps open` expose the refreshed state and available reload/restart actions.
- Update docs/usage only where needed to make the local dev-mode reload path discoverable from the persistent local runtime workflow.

## Non-scope

- No hosted marketplace, background auto-update daemon, package watcher, git fetch/clone updater, remote pin resolver, or scheduled update polling.
- No broad package registry redesign, new plugin lifecycle abstraction, or rewrite of app/open/status projection.
- No frontend SPA or Project Pipelines UI changes unless a generated TypeScript DTO is mechanically required by the public daemon protocol.
- No migration of existing package state beyond what is required to re-read local-path packages in the current persisted snapshot format.
- No raw local path exposure in normal CLI output beyond explicit user-supplied command arguments or existing data-dir command hints.

## Assumptions and unknowns

- Assumption: "after edits" means operator-triggered reload, not file-watch hot reload. The ticket explicitly says no auto-update daemon.
- Assumption: local package source can be recovered from the persisted manifest/provenance/source metadata for packages installed through `InstallPackageLocalPath` or `EnablePackageLocalPath`. If current records do not preserve enough path data, add the smallest source-root field needed to persisted local package records and reconstruct it during install going forward.
- Assumption: package name changes during reload should fail with a diagnostic instead of silently replacing one package with another.
- Assumption: configuration values should survive reload when the refreshed manifest still declares matching fields; invalid or newly missing required configuration should block re-enable/reload with diagnostics.
- Unknown: whether "build artifact" is already represented in manifests available from the locked `botster-core` revision. Implementer must inspect current core manifest structs before adding any hub-local field. If no build-artifact contract exists, scope reload to re-reading declared runnable output/readiness files and document scaffold status.
- Unknown: whether generated TypeScript protocol drift checks are required in this repo for every daemon enum change. If the request enum changes, update the generated file or run the repo's existing generator.
- Worktree/target assumption: implementation agents must work in their assigned Project Pipelines worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`, not an ambient checkout.

## Affected surfaces/files

- `src/packages.rs`
  - likely add `PackageRegistry::reload_local_package` or equivalent narrow method that reads the existing local package source, validates the refreshed manifest/runnable entrypoints/session templates/config schema, preserves mutable state, and returns a package decision/status.
- `src/daemon_transport.rs`
  - add `DaemonRequest::ReloadPackage` handling in the daemon owner thread; persist refreshed registry; reload enabled Lua lifecycle; restart running entrypoints; return package/app diagnostics through existing response fields.
- `src/entrypoint_supervisor.rs`
  - likely add a helper to list running entrypoint ids for one package, or reuse `snapshots()` and restart from the daemon path.
- `crates/botster-hub-client/src/lib.rs`
  - add the daemon request variant, request kind label, serialization tests, and package action/status fields if needed.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`
  - update generated protocol mirrors if the public daemon contract changes.
- `src/main.rs`
  - parse and dispatch `packages reload <name>`; update usage and print bounded diagnostics/action output.
- `README.md`
  - optional narrow docs for local runtime local reload if command usage alone is insufficient.
- `tests/hub_daemon_lifecycle_test.rs`
  - add real daemon tests for local package reload, app projection, entrypoint restart, and diagnostic failures.

## Implementation plan

1. Add the package registry refresh primitive.
   - Resolve only installed local-path packages.
   - Re-read `botster-package.json` from the same package root/manifest path.
   - Validate the refreshed manifest with the same local install validators.
   - Require refreshed manifest name to match the installed package name.
   - Preserve state/config/trust/provenance/source metadata where appropriate; re-derive compatibility, runnable entrypoints, session templates, capability admission, and host-profile admission through existing helpers.

2. Add the daemon request path.
   - Add `ReloadPackage { package_name }` to `DaemonRequest`.
   - In `handle_control_request`, snapshot currently running entrypoints for that package, call the registry reload primitive, persist state, reload Lua lifecycle when enabled, and restart only previously running supervised entrypoints.
   - Return one `DaemonResponse` that includes the package row/decision and diagnostics from manifest validation, lifecycle reload, build/output/readiness, and entrypoint restart attempts.

3. Wire CLI DX.
   - Add `packages reload <name>` to `PackageCommand::parse`.
   - Keep output consistent with `packages show` and existing package diagnostics.
   - Update usage text and, if useful, local runtime ready output to point developers at `packages reload --data-dir <dir> <package>`.

4. Prove the actual user path.
   - Real daemon test: enable a local package, mutate its manifest/runnable entrypoint/surface metadata at the same path, run `packages reload`, then assert `packages show/list` and `apps list` reflect the new package state.
   - Running app test: start an app entrypoint, mutate its command/output/readiness behavior, reload, assert the old process exits, the replacement process starts, and `apps open`/`apps list` reports the new local URL/status.
   - Failure tests: invalid manifest/build output/readiness failure returns bounded diagnostics and does not leak local package source paths in normal output.

## Risks

- Persisted source-path risk: existing local installs may not persist enough source-root information to reload. Mitigate by adding the smallest path-backed source metadata needed and documenting behavior for older records as "reload unavailable".
- Lifecycle ordering risk: reloading the registry before lifecycle reload could leave persisted enabled state for a package that fails to reload. Mitigate with a clear diagnostic and avoid killing the old running lifecycle until a replacement can be loaded, where core lifecycle semantics permit it.
- Entrypoint restart risk: restarting all entrypoints could disrupt unrelated package processes. Mitigate by filtering snapshots by package name and previously running state.
- App projection risk: `ListApps` can show stale local URLs if retained supervisor snapshots are not refreshed after restart. Mitigate by asserting `apps list` after reload in the real daemon path.
- Protocol drift risk: adding a public daemon request requires client DTO and TypeScript mirror updates. Mitigate with existing protocol serialization/generation tests.
- PII/path leakage risk: diagnostics and plan/report artifacts can leak local absolute paths. Mitigate with bounded messages and explicit scans.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test <new_reload_test_name> -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test <new_reload_running_entrypoint_test_name> -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test <new_reload_failure_diagnostics_test_name> -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_bootstrap_starts_daemon_enables_first_party_packages_and_prints_apps -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_restarts_running_processes -- --test-threads=1`
- If protocol structs change: run the botster-hub-client protocol/type generation or drift test used by this repo, and update `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Static leak scan before review: search committed artifacts for absolute home paths, session worktree paths, and user-identifying strings.
- Implementation evidence must identify the production entrypoint: `botster-hub packages reload` -> `DaemonRequest::ReloadPackage` -> daemon owner `handle_control_request` -> `PackageRegistry` refresh -> lifecycle reload/entrypoint restart -> `ListApps`/`apps open` reads refreshed state.

## Pipeline gates and artifacts

- This file is the Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Plan gate evidence should attach this plan and the checklist fallback evidence because checklist creation timed out.
- Downstream Implement evidence must include changed files, committed diff summary, exact tests run, production path proof, PII scan result, and any deferred diagnostics.

## Vault gaps worth capturing

- Capture candidate if implementation settles a durable convention for local package reload semantics: "dev-mode package reload is explicit operator action, not file watcher policy".
- Capture candidate if source-root persistence for local packages becomes a reusable rule: "local package records must retain a sanitized reload source handle".
- Capture candidate if build artifact/readiness diagnostics require a durable manifest contract note after inspecting the locked `botster-core` package schema.
