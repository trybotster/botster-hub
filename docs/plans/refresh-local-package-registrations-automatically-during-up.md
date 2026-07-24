# Refresh local package registrations automatically during up

## Target repository and pipeline context

- Target repository: `botster-hub`.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1784844649_497446` — “Hub: refresh local package registrations automatically during up”.
- Run/step: `run_1784844668_758792` / `botster_stack_plan`.
- Assigned worktree: the pipeline-created `botster-hub` worktree for this ticket. Implement and verify here, not in an ambient checkout.
- Current pipeline context had no dependencies, prior artifacts, reviews, findings, or open questions.

## Repository and role guidance loaded

- Role playbooks: [[planner-playbook]], then [[botster-planner-playbook]].
- Repository ownership charter: [[botster-hub-playbook]].
- Surface/review guidance: [[botster-package-reviewer-playbook]], [[botster-package-verifier-playbook]].
- Project Pipelines workflow charter: [[project-pipelines-playbook]] was loaded only for this run's checklist, gate, artifact, and advancement discipline; no Project Pipelines product/package code is in scope.
- Architecture maps and required Botster planning context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[botster pipeline needs continuous product owner between agent steps]].
- Hub ownership notes: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster hub events use bounded priority lanes instead of unbounded queue fuses]].
- Targeted package/runtime notes: [[botster package registry persists through hub state json]], [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster package manifest validation requires hub compiled core revision]], [[botster runnable entrypoints are hub owned launch contracts]], [[botster host injected runtime paths are absolute before package cwd boundaries]], [[local runnable packages still need core entrypoint for enable prepare]], [[botster plugins reload through mcp not file watching]], [[botster nested plugin module updates may need explicit reload]], and [[installed apps are daemon app rows projected from package runnable entrypoints]].
- Repository context inspected: `README.md`, `test.sh`, `src/main.rs`, `src/packages.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/runtime.rs`, `src/persistence.rs`, `src/entrypoint_supervisor.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `tests/hub_daemon_lifecycle_test.rs`, and the prior package reload, entrypoint-contract, supervision, daily-flow, and canonical-data-dir plans.

## Current production path

`botster-hub up` calls `prepare_local_runtime`, which starts or reuses the daemon, enables the selected first-party package paths, and starts `botster-web/web-client`. A newly started `HubDaemon` currently restores `HubState.package_registry`, constructs `PackageRegistry` from that snapshot, and immediately loads enabled local Lua packages from the durable records. A reused daemon keeps its in-memory records. The daily enable helper falls back from `EnablePackageLocalPath`'s already-installed result to `EnablePackage`, so it does not reread the installed checkout.

Explicit `ReloadPackage` already routes through the live daemon owner to `PackageRegistry::reload_local_package`, persists the refreshed registry, reloads enabled Lua lifecycle, and restarts running supervised entrypoints. The missing behavior is a transactional all-local refresh used by both startup/`up` and this explicit single-package path.

## Scope

- Add one package-registry refresh implementation that can prepare either one named directly installed path package or every directly installed path package.
- Build every refreshed record in a candidate registry using the existing manifest parser, core-entrypoint validation, runnable-entrypoint/session-template validation, compatibility checks, configuration preservation, and enable/admission helpers. Commit the candidate records only after all selected packages validate.
- Distinguish direct local-path installs from registry-installed packages using existing provenance/source metadata, not merely `PackageSource::Path`; registry-installed records remain pinned and unchanged.
- During daemon startup, refresh and atomically persist the complete candidate registry before loading any enabled local Lua package.
- During every `up` against a reused daemon, invoke one daemon-owned batch refresh before daily package enablement or app launch. Reload enabled local Lua lifecycles and restart only entrypoints that were already running after the batch registration commits.
- Refactor explicit `reload <package>` / `packages reload` to call the same candidate-record refresh implementation while retaining its current operator command and targeted runtime activation behavior.
- Validate missing package roots/manifests/core entrypoints and missing package-relative runnable command/build outputs during refresh. The failure must identify the package and local path and, for missing declared output, explicitly say the package needs rebuilding.
- Keep the durable registry and in-memory daemon/runtime state aligned after a successful atomic save.
- Update daily-flow and troubleshooting documentation so bare `up` is the normal refresh path, explicit reload remains available, and neither path builds sibling package artifacts.

## Non-scope

- No file watcher, background update poller, implicit registry/Git fetch, pin update, package build runner, dependency solver, or marketplace redesign.
- No `botster-core`, `botster-hub-client` consumer-repository, `botster-web`, `botster-tui`, `botster-workspaces`, or Project Pipelines package implementation changes.
- No compatibility launch path, versioned refresh API, dual registry, or migration of package records to a second state file.
- No broad refactor of `HubDaemon`, lifecycle, entrypoint supervision, app projection, or package DTOs beyond the request/result fields needed for the batch refresh.
- No restart of unrelated package entrypoints and no automatic launch of an entrypoint that was not already running or selected later by `up`.

## Ownership boundaries and cross-repository dependencies

- `botster-hub` owns this change because it owns package install/enable/update policy, durable `hub-state.json`, startup ordering, plugin lifecycle, entrypoint supervision, daemon requests, and daily CLI composition.
- `botster-core` continues to own reusable manifest and admission contracts. The implementation must use the exact core revision locked by this Hub checkout and must not add a parallel Hub manifest schema for this ticket.
- `botster-hub-client` protocol types embedded in this repository own any new daemon request/result shape. Checked-in generated TypeScript must stay synchronized, but no downstream client behavior change is required.
- Package repositories own their manifests and build outputs. Hub only rereads and validates; it does not build them.
- No blocking cross-repository prerequisite is currently identified. If implementation proves the locked core contract cannot express the required validation, stop and register a dependency against `botster-core` rather than adding a Hub-only compatibility contract.

## Assumptions and unknowns

- “Local path package” means a package installed directly from a filesystem path. A registry installation remains registry-owned and pinned even when its catalog entry resolves to a local path; existing `source_metadata`/provenance is the discrimination boundary.
- “One coherent pre-launch operation” means all selected registration reads and validations succeed before any candidate record is committed, persisted, lifecycle-reloaded, or newly launched. Runtime reload/restart happens only after this registration barrier.
- An `up` that reuses a daemon must refresh through the daemon owner; the short-lived CLI must not edit `hub-state.json` directly.
- Missing package-relative runnable commands are declared local build-output failures and should be rejected before launch with rebuild guidance. Bare host `PATH` commands cannot be proven during manifest refresh and retain supervisor-time diagnostics.
- Ticket-required failure output may include the affected package's stored local path. This is a deliberate exception for failed `up`/reload diagnostics; normal package, status, and app DTO output remains path-sanitized.
- Existing configuration, trust, enabled/disabled state, timestamps, and direct-local provenance survive refresh; manifest-derived compatibility, capabilities, host-profile admission, runnable entrypoints, and session templates are recomputed.
- Unknown to resolve during implementation: whether the batch daemon response needs new structured per-package decision rows or can use existing package decisions plus diagnostics without weakening precise failure reporting. Prefer the smallest additive protocol shape.

## Affected surfaces and files

- `src/packages.rs`
  - Extract candidate-record construction from `reload_local_package`.
  - Add direct-local selection and transactional single/all refresh APIs.
  - Add package-relative runnable command/output existence validation with rebuild diagnostics.
  - Unit-test multiple-package atomicity, direct-local versus registry-source discrimination, preserved mutable state, and missing path/output errors.
- `src/daemon.rs`
  - Refresh/persist the durable package snapshot before `load_enabled_local_plugins`.
  - Ensure startup failure leaves the prior committed registry intact and includes package/path context.
- `src/daemon_transport.rs`
  - Add the daemon-owned batch refresh request for reused `up`.
  - Persist once after the candidate batch commits, synchronize daemon state, then reload enabled Lua packages and restart only previously running package entrypoints.
  - Route explicit reload through the same registry primitive.
- `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, and `crates/botster-hub-client/generated/daemon-protocol.ts`
  - Add and prove the minimal serde/TypeScript daemon request or response additions required for batch refresh.
- `src/main.rs`
  - Insert the batch refresh barrier in `prepare_local_runtime` after daemon start/reuse and before first-party enable/start operations.
  - Preserve `reload <package>` and existing package command output.
- `src/persistence.rs`
  - Extend atomic state-store coverage if needed to prove a failed batch or failed save retains the prior complete snapshot.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add real binary/daemon acceptance for stopped and reused daily runtimes, atomic multi-package failure, registry-package stability, and diagnostics.
- `README.md`
  - Replace “reload is required after edits” daily guidance with automatic `up` refresh, retain explicit reload documentation, and state that missing build output requires rebuilding.

## Implementation plan

1. Refactor package refresh around an all-or-nothing candidate registry.
   - Identify direct local installs from current record metadata.
   - Reuse the exact local manifest and admission validators already used by install/reload.
   - Prepare all requested replacement records against a cloned registry and only swap records after the entire set passes.
   - Have explicit single-package reload delegate to this implementation.

2. Put the refresh barrier before runtime activation.
   - On daemon startup, restore the snapshot, prepare the full direct-local refresh, atomically save the updated `HubState.package_registry`, then construct/load runtime plugins from that committed registry.
   - On a reused daemon, handle one batch-refresh daemon request. Snapshot currently running entrypoints first; after a successful registry commit, reload enabled Lua packages and restart only those prior live entrypoints using refreshed declarations.
   - Do not reload, restart, persist, or launch anything when any package fails preparation.

3. Wire the daily and explicit operator paths.
   - Call the batch daemon request from `prepare_local_runtime` before resolving and starting persisted package entrypoints.
   - Keep explicit reload commands and action descriptors unchanged externally, backed by the shared refresh primitive.
   - Keep registry-installed records out of implicit refresh regardless of their catalog source representation.

4. Add precise diagnostics and documentation.
   - Include package name and stored path for missing checkout/manifest/entrypoint failures.
   - Classify a missing package-relative runnable command as missing declared build output and instruct the operator to rebuild before rerunning `up` or explicit reload.
   - Update README daily flow/troubleshooting without implying builds, watchers, or registry updates.

5. Prove the actual user path.
   - Drive the compiled `botster-hub up` binary against durable state containing an old local manifest, mutate the checkout entrypoint, and assert the new entrypoint launches without reload, reinstall, or data-dir deletion.
   - Exercise both daemon-started and daemon-reused `up` paths.

## Risks

- Startup ordering: refreshing after `HubRuntime` loads enabled plugins would already execute stale code. The state refresh/save barrier must precede `load_enabled_local_plugins`.
- Partial activation: committing records one package at a time, or reloading lifecycle while later packages are still validating, can create a mixed launch set. Candidate preparation and one persistence commit must finish first.
- Source classification: filtering only on `PackageSource::Path` could implicitly update registry-installed local catalog entries. Tests must cover this exact distinction.
- Persistence/in-memory drift: current package persistence helpers do not always replace the daemon's cached aggregate state. The new path must prove `HubDaemon`, `HubRuntime`, and `hub-state.json` agree after commit.
- Reused-daemon behavior: startup-only refresh would not satisfy “every up.” The live daemon request and production binary test are required.
- Runtime follow-through: refreshed registrations without Lua reload or supervised-entrypoint restart would leave running behavior stale. Tests must inspect the launched marker/app state, not only package DTOs.
- Diagnostic leakage: the ticket requires the failing package path, while normal output intentionally hides paths. Limit path disclosure to actionable refresh failure diagnostics.
- Protocol drift: an additive daemon request requires serde request-kind coverage and regenerated checked-in TypeScript.
- Missing-output ambiguity: only package-relative commands can be reliably classified as local build outputs during refresh; do not falsely reject legitimate bare `PATH` commands.

## Acceptance checks and downstream proof

- Focused package unit tests through the repository wrapper:
  - `./test.sh packages::tests::<batch_refresh_success_test>`
  - `./test.sh packages::tests::<batch_refresh_atomic_failure_test>`
  - `./test.sh packages::tests::<registry_installed_package_unchanged_test>`
- Real daemon/binary tests through the repository wrapper:
  - `./test.sh --test hub_daemon_lifecycle_test <up_refreshes_stale_local_manifest_before_launch> -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test <reused_daemon_up_refreshes_all_local_packages_before_launch> -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test <failed_batch_refresh_preserves_old_complete_registry_and_launch_set> -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test <up_leaves_registry_installed_package_pinned> -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test <up_reports_missing_local_checkout_with_package_and_path> -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test <up_reports_missing_declared_build_output_with_rebuild_guidance> -- --test-threads=1`
  - Existing `local_package_reload_rereads_manifest_restarts_running_app_and_cli_open_uses_refreshed_state` remains green to prove explicit reload behavior.
- Public protocol proof: `./test.sh -p botster-hub-client`, including checked TypeScript drift and request serde tests.
- Required complete repository verification: `./test.sh` (not raw `cargo test`).
- Strict Rust gates required by the Hub charter: `cargo fmt --check` and `cargo clippy --all-targets --all-features -- -D warnings`; attribute any failure exactly rather than treating a pre-existing failure as a blanket waiver.
- Runtime success evidence must show the production chain:
  `botster-hub up` → daemon start/reuse → atomic direct-local batch refresh/persist → enabled Lua lifecycle activation/reload → supervised `botster-web` start/restart → new entrypoint marker/app state.
- Verify startup, reuse, shutdown, immediate restart, and cleanup regressions with the existing daily runtime lifecycle tests in addition to the new focused tests.

## Pipeline artifacts and checklist evidence

- Plan artifact: this file.
- Run checklist: `checklist_1784845093_409282`.
- The initial checklist creation call timed out in the plugin worker, but durable inspection confirmed the checklist and its four standard items were created. Checklist evidence records notes loaded, convention review, planned repository-wrapper verification, and the post-implementation capture decision.
- Plan gate evidence must attach this artifact and all required fields. Implement/Review/Verify evidence must include committed diff/PR state, exact wrapper commands and results, production-path proof, resolved findings, and any verification gaps.

## Convention conflicts

- No architecture conflict. The change remains Hub-owned startup/package policy, uses the existing manifest/admission and atomic file-store primitives, preserves one production path, and adds no watcher or compatibility path.
- [[botster plugins reload through mcp not file watching]] is not violated: this ticket adds refresh at an explicit `up` lifecycle boundary and preserves explicit reload; it does not restore file watching.
- The ticket intentionally narrows the normal path-sanitization rule for refresh failures by requiring the affected local path. Normal read/status projections remain sanitized.
- The previous daily documentation said developers must explicitly reload after edits. The ticket supersedes that operational rule for `up`; explicit reload remains an operator command.

## Vault gaps worth capturing

- After implementation verifies the semantics, capture a durable note equivalent to “botster up transactionally refreshes direct local package registrations before launch while registry packages remain pinned.”
- Enrich the explicit-reload/no-file-watcher note to distinguish lifecycle-triggered `up` refresh from watcher-driven reload.
- Capture the durable diagnostic rule if implementation confirms it: missing package-relative runnable commands are stale build output and must name the package/path plus rebuild remediation.
- No vault write is appropriate during Plan because these behaviors are ticket intent, not yet verified implementation knowledge.
