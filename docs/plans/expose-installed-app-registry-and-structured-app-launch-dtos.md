---
ticket: ticket_1782361545_680661
title: Expose installed app registry and structured app launch DTOs in botster-hub
run: run_1782364556_708985
step: botster_plan
---

# Expose Installed App Registry and Structured App Launch DTOs

## Context loaded

- Pipeline context: ticket `ticket_1782361545_680661`, run `run_1782364556_708985`, current step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, questions, or answers were present. The dependency `ticket_1782361545_165494` ("Define client app entrypoint contract in botster-core package manifests") is closed.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster/vault constraints: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[botster runnable entrypoints are hub owned launch contracts]], [[local runnable packages still need core entrypoint for enable prepare]], [[botster package daemon dto exposes sanitized package rows]], [[botster hub client crate is the external client boundary]], [[generated typescript dtos must encode serde field optionality]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Skill loaded: `botster-customize-hub`, because this ticket changes hub daemon behavior and public client DTOs.
- Checklist discipline: `project_pipelines_checklist_instructions` loaded. `project_pipelines_create_vault_checklist` initially returned `plugin worker invoke timeout`, but the checklist record persisted as `checklist_1782364599_737937`; update evidence should be kept both on checklist items and in this plan/gate evidence.
- Repo context inspected: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, `src/packages.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/entrypoint_supervisor.rs`, `src/main.rs`, `docs/client-protocol.md`, `tests/hub_client_api_test.rs`, and `tests/hub_daemon_lifecycle_test.rs`.
- Current implementation shape: installed packages already expose sanitized `DaemonPackage.runnable_entrypoints`, start/stop/restart/status daemon requests, live supervisor process snapshots, action descriptors, and generated TypeScript. There is no current first-class installed app registry or `apps` DTO family. There is no structured launch target such as `local_url`; any web URL is currently discoverable only through process output/diagnostics or launcher-specific paths.
- Plan Review correction: the registered dependency ticket is closed and merged in `botster-core`, but this hub worktree pins `botster-core` and `botster-core-daemon` at `2eafcee`, before the core client-app entrypoint contract. Core origin/main authoritatively defines `RunnableEntrypoint`, `RunnableEntrypointKind` (`web_app`, `terminal_app`), `RunnableEntrypointLaunchMode` (`background`, `foreground_stdio`), `RunnableEntrypointReadiness.result_fields`, `RunnableEntrypointResultField::LocalUrl`, and `RunnableEntrypointLaunchResult { entrypoint_id, process_state, local_url }`. Implementation must consume that core contract rather than the hub's older local vocabulary.

## Scope

- First, update the `botster-core` / `botster-core-daemon` git dependency pin to a revision containing the core `RunnableEntrypoint*` contract, then verify those symbols resolve before DTO work begins.
- Reconcile the hub's package runnable-entrypoint model to core's `RunnableEntrypoint` contract. This should be a cold-turkey replacement of the older hub-local kind/mode vocabulary (`Client`/`Web`/`Mcp`/`Daemon`/`Provider`, `dev`/`local`) with core's authoritative `web_app`/`terminal_app` and `background`/`foreground_stdio`, unless implementation discovers a real persisted compatibility boundary that requires a narrowly documented migration shim.
- Add a hub-owned installed app projection over installed package runnable entrypoints. Each app row should be derived from one installed package entrypoint, not from client-side inference.
- Add public `botster-hub-client` DTOs for installed apps with the ticket-required fields:
  - `package_name`
  - `app_id`
  - `entrypoint_id`
  - `kind`
  - `launch_mode`
  - `lifecycle_state`
  - `diagnostics`
  - `actions`
  - `blocked_reasons`
  - structured launch target fields, with `local_url` present for a running web app when known
- Add daemon request/response shape for listing installed apps, likely `DaemonRequest::ListApps` and `DaemonResponseKind::Apps` with `apps: Vec<DaemonApp>`.
- Keep the app registry host-owned and authoritative. CLI/TUI/browser clients should consume the app DTO or generated TypeScript protocol artifact and should not parse URLs out of diagnostics once `local_url` exists.
- Extend supervisor state with a sanitized structured launch target. For web apps, populate `DaemonAppLaunchTarget.local_url` only from core `RunnableEntrypointLaunchResult.local_url` when the entrypoint readiness declares `RunnableEntrypointResultField::LocalUrl`. For terminal apps, return a terminal-shaped launch target without pretending there is a background URL.
- Reuse existing package action vocabulary where possible. App `actions` can map to existing package entrypoint lifecycle requests (`start_package_entrypoint`, `stop_package_entrypoint`, `restart_package_entrypoint`, `package_entrypoint_status`) rather than inventing parallel command names unless the DTO shape needs app-specific request metadata.
- Thread the app projection through production daemon paths: package registry + entrypoint supervisor snapshots -> hub/client API or daemon projection helper -> `DaemonResponse.apps` -> generated TypeScript.
- Update protocol docs to document the app registry and the structured launch-target rule.

## Non-scope

- No final friendly CLI commands or high-level UX workflows.
- No client-side inference, URL parsing from diagnostics, or generated DTO consumers outside this repo.
- No broad package registry rewrite, app manager abstraction, marketplace policy rewrite, or plugin lifecycle refactor.
- No new process supervisor unless the existing `EntrypointSupervisor` cannot carry the minimal structured launch target.
- No automatic app launch on install/enable beyond existing explicit start behavior.
- No PII-bearing local paths, socket paths, raw provenance paths, host environment dumps, or secret values in app DTOs.

## Assumptions and unknowns

- Assumption: the core dependency contract is merged and registered, but the hub lockfile is stale. Implementation should update `Cargo.lock` for `botster-core` and `botster-core-daemon` before changing DTOs.
- Assumption: `app_id` should come from the core `RunnableEntrypoint` id with uniqueness provided by `(package_name, app_id)`. If clients need a globally unique string, expose an additional deterministic composite only if the public contract requires it; do not replace the core id.
- Assumption: `kind` must use core `RunnableEntrypointKind` serialized vocabulary: `web_app` and `terminal_app`.
- Assumption: `launch_mode` must use core `RunnableEntrypointLaunchMode` serialized vocabulary: `background` and `foreground_stdio`.
- Assumption: `lifecycle_state` should use the existing process state vocabulary (`not_started`, `running`, `exited`, `failed`, `stopped`) unless core introduces a narrower app lifecycle enum.
- Assumption: `blocked_reasons` should reuse package availability/config/dependency reasons plus entrypoint-specific blocked states such as package disabled, missing required configuration, not supervisable, unsupported launch mode, or launch result unavailable.
- Assumption: `local_url` is not unknown. It must flow from core `RunnableEntrypointLaunchResult.local_url`, gated by `RunnableEntrypointReadiness.result_fields` containing `LocalUrl`. If the supervisor has no launch result yet, return `local_url: null` or omit it according to serde shape; do not synthesize it from environment defaults, stdout, stderr, diagnostics, command arguments, or known package names.
- Unknown: whether app rows should live on `DaemonResponse.apps` only or also be embedded on `DaemonPackage`. Prefer a top-level list request for a first-class registry while leaving `DaemonPackage.runnable_entrypoints` intact for package management.

## Botster layers touched

- Rust hub package/app projection layer.
- Rust entrypoint supervisor state snapshot layer.
- Rust daemon socket DTO layer in `botster-hub-client`.
- Generated TypeScript daemon protocol artifact.
- Thin CLI only for existing debug/show paths if tests need a human-readable proof; friendly commands remain out of scope.
- Protocol docs and integration tests.

No Lua plugin policy, Project Pipelines plugin workflow, React SPA implementation, Rails relay, or TUI UI work is required.

## Affected surfaces/files

- `Cargo.lock`
  - Update `botster-core` and `botster-core-daemon` from the stale `2eafcee` pin to a revision containing `RunnableEntrypoint`, `RunnableEntrypointKind`, `RunnableEntrypointLaunchMode`, `RunnableEntrypointReadiness`, `RunnableEntrypointResultField`, and `RunnableEntrypointLaunchResult`.
- `Cargo.toml`
  - No dependency shape change expected unless the core crate exposes the app contract behind a feature; keep the existing git main dependency pattern.
- `src/packages.rs`
  - Replace/reconcile the hub-local `PackageRunnableEntrypoint*` model with core `RunnableEntrypoint*` types and serialized vocabulary. Avoid a long-lived translation layer between old and new kind/mode values unless old persisted `hub-state.json` rows require a narrow one-time serde migration/default.
- `crates/botster-hub-client/src/lib.rs`
  - Add `DaemonRequest::ListApps`.
  - Add `DaemonResponse.apps`.
  - Add `DaemonApp`, `DaemonAppLaunchTarget`, app diagnostics, blocked reasons, and any app action request DTOs needed to preserve structured request metadata.
  - Add serde tests for web app with `local_url`, terminal app without URL, legacy responses without `apps`, and omitted optional fields.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Generate/check the app DTOs and request/response variants, preserving serde optionality.
- `src/entrypoint_supervisor.rs`
  - Extend `EntrypointProcessSnapshot` with core `RunnableEntrypointLaunchResult`-backed state if the supervisor owns runtime launch results.
  - Preserve `process_state` and `local_url` from the core launch result rather than deriving URL fields from output or environment.
  - Keep diagnostics sanitized and avoid local path/env leaks.
- `src/client_api.rs`
  - Either add a transport-neutral `HubClientApp` projection or keep the projection in daemon transport if it is purely socket DTO shaping. Prefer `HubClientApi` only if another local client path should share the app registry.
- `src/daemon_transport.rs`
  - Add request handling for `ListApps`.
  - Derive app rows from installed package core runnable entrypoints plus `entrypoint_supervisor().snapshots()` / launch results.
  - Ensure app actions and blocked reasons reflect package state, availability, required configuration, and supervisor state.
  - Ensure `ListPackages`/`ShowPackage` continue to expose package entrypoints without becoming the only app registry.
- `src/main.rs`
  - Only update existing status/debug output if needed for a covered acceptance proof. Do not add final friendly app commands.
- `docs/client-protocol.md`
  - Document installed app registry DTOs, launch target semantics, and the no-diagnostics-parsing rule.
- `tests/hub_client_api_test.rs`
  - Add projection tests if app projection is shared through `HubClientApi`.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add live daemon tests proving local packages project into app DTOs, web app `local_url` is structured when running, terminal apps do not expose fake URLs, and generated/client DTOs carry the authoritative protocol shape.

## Risks

- Stale dependency risk: this worktree currently pins `botster-core` at `2eafcee`, which predates the merged app-entrypoint contract. Implementation must update the core pins first and prove symbols resolve.
- Contract drift risk: local DTOs could fork the core contract if they retain hub-local `web`/`client` or `dev`/`local` values. Mitigation: use core `RunnableEntrypoint*` vocabulary verbatim and remove the older hub-local vocabulary in the same change unless a real persistence boundary requires a targeted shim.
- Underwiring risk: adding structs without routing `ListApps` through the running daemon would fail the ticket. Tests must prove the production request path.
- URL source risk: deriving `local_url` by parsing process output or environment defaults would recreate the exact client-inference problem in hub code. Use only `RunnableEntrypointLaunchResult.local_url`; if no launch result exists, return no URL with an explicit lifecycle/blocked reason.
- Terminal app risk: terminal entrypoints must be represented as terminal apps with lifecycle/actions but no background URL.
- Optionality risk: generated TypeScript must mark skipped/nullable fields optional where serde can omit them.
- Privacy risk: app DTO diagnostics and launch targets must not leak local package roots, data dirs, socket paths, HOME, or host-resolved environment values.
- Scope creep risk: "app registry" could expand into friendly commands or browser UI. Keep this ticket to daemon protocol/API shape and host policy/state.

## Acceptance checks/tests

- `cargo update -p botster-core -p botster-core-daemon` or equivalent lockfile update to a revision containing the merged core runnable-entrypoint app contract.
- Compile/type proof that `botster_core::RunnableEntrypoint`, `RunnableEntrypointKind`, `RunnableEntrypointLaunchMode`, `RunnableEntrypointReadiness`, `RunnableEntrypointResultField`, and `RunnableEntrypointLaunchResult` resolve from the updated dependency.
- `cargo fmt`
- `cargo test -p botster-hub-client` or focused tests covering:
  - `DaemonRequest::ListApps` serde shape.
  - `DaemonResponse.apps` serde default/omission behavior.
  - web app DTO with structured `launch_target.local_url`.
  - terminal app DTO with terminal launch target and no URL.
  - generated TypeScript drift check.
- `./test.sh --test hub_daemon_lifecycle_test <focused installed app registry test>`
  - Install/enable a local package with core `web_app` and `terminal_app` entrypoints.
  - Assert `ListApps` returns package name, app id, entrypoint id, kind, launch mode, lifecycle state, diagnostics, actions, blocked reasons, and launch target fields.
  - Start the web entrypoint and assert `local_url` in the app DTO comes from the supervisor's core `RunnableEntrypointLaunchResult.local_url` for a `web_app` whose readiness `result_fields` includes `LocalUrl`.
  - Assert a `terminal_app` with `foreground_stdio` is represented with terminal launch semantics and no background `local_url`.
- Existing package lifecycle tests around `StartPackageEntrypoint`, `StopPackageEntrypoint`, `PackageEntrypointStatus`, `ListPackages`, and `ShowPackage` should still pass.
- If `src/daemon_transport.rs` or shared DTO mappings change broadly, run `./test.sh` and strict `cargo clippy --all-targets --all-features -- -D warnings`, attributing any baseline failures to touched vs untouched files.
- Documentation check: `docs/client-protocol.md` says clients consume app DTOs/generated protocol artifacts and must not parse URLs from diagnostics.
- Production path proof required in implementation report: `DaemonRequest::ListApps` enters through `src/daemon_transport.rs`, reads `PackageRegistry` and `EntrypointSupervisor` state, projects `DaemonApp` rows from installed package entrypoints, and returns them in `DaemonResponse.apps`.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Gate evidence should include loaded context, scope/non-scope, assumptions/unknowns, affected surfaces/files, risks, acceptance checks, and vault gaps.
- Checklist evidence should name the loaded vault notes, record no convention conflict, list this plan artifact as Plan-stage verification, and defer durable vault capture until implementation confirms the final app DTO/launch target vocabulary.

## Worktree and target assumptions

- Work happens in the pipeline-assigned ticket worktree for `ticket_1782361545_680661`.
- Run target is `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Agents must use the assigned worktree and target identity, not an ambient checkout.

## Vault gaps worth capturing

- Capture after implementation whether Botster's first-class installed app registry is a top-level daemon DTO derived from `runnable_entrypoints`, and record the stable app id/key rule.
- Capture that core owns the runnable app-entrypoint vocabulary: `web_app`/`terminal_app`, `background`/`foreground_stdio`, readiness result fields, and `RunnableEntrypointLaunchResult.local_url`.
- Capture the structured launch target source of truth, especially that `local_url` is populated only from `RunnableEntrypointLaunchResult.local_url` and diagnostics/output/env parsing is forbidden.
- Capture whether app blocked reasons reuse package availability/action diagnostics or establish a distinct app availability vocabulary.
