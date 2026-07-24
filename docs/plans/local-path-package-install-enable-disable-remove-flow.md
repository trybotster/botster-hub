# Local Path Package Install Enable Disable Remove Flow

## Context Loaded

- Ticket: `ticket_1781054950_975598` / "Implement local path package install enable disable remove flow".
- Run: `run_1781058150_503596`; step `botster_plan`; gate `botster_plan_gate`.
- Dependency: "Define local package manifest lockfile and registry contracts" is closed.
- Pipeline context: no prior artifacts, reviews, findings, questions, or answers.
- Playbooks and vault notes: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], plus `identity` and `goals`.
- Checklist discipline: attempted `project_pipelines_create_vault_checklist`; the plugin worker timed out. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and the gate payload carry the notes-read, convention, verification, and capture evidence.

## Existing Runtime Shape

- `src/packages.rs` already owns manifest parsing, local path canonicalization, compatibility checks, capability admission, disabled/enabled states, durable snapshots, and local Lua entrypoint preparation.
- `src/daemon_transport.rs` already routes package mutations through the running daemon owner, persists `hub-state.json`, refreshes package reads after mutation, and loads/unloads Lua plugin packages.
- `src/main.rs` exposes a thin `packages` and `providers` CLI over daemon transport. Current verbs are `list`, `enable --path`, `enable <name>`, and `disable <name>`.
- `README.md` documents the current daemon-backed production runtime path, but only for combined install+enable through `packages enable --path`.
- `tests/hub_daemon_lifecycle_test.rs` already proves `packages enable --path` persists and loads a local package through the running daemon.

## Scope

- Add explicit local package lifecycle verbs:
  - `packages install --data-dir <dir> --path <local-package-or-manifest>` installs a local package as disabled.
  - `packages enable --data-dir <dir> <package-name>` enables an installed package and loads local Lua plugin lifecycle.
  - `packages disable --data-dir <dir> <package-name>` disables and unloads.
  - `packages remove --data-dir <dir> <package-name>` removes the package record and unloads first if needed.
  - `packages list --data-dir <dir>` lists sanitized package records.
  - `packages show --data-dir <dir> <package-name>` or equivalent status command returns one sanitized package record plus structured diagnostics when absent.
- Keep package mutations daemon-owned. CLI commands should fail when no running daemon is available and must not create or mutate `hub-state.json` offline.
- Persist state through the existing hub-state package registry snapshot contract, reusing closed dependency APIs already present in this worktree.
- Extend typed daemon/client DTOs only as needed for install, remove, and show/status. Keep the CLI a thin adapter.
- Emit structured diagnostics/events for install success, validation failure, compatibility mismatch, capability denial, enable/disable/remove, and load failure using existing `DaemonDiagnostic`, `DaemonOperatorError`, `PackageDecision`, and package error shapes where possible.
- Add or refine automated tests for the real user path from an isolated local package path and disposable data dir.
- Update manual production runtime docs with exact install, list, show, enable, disable, remove commands.
- Keep the checked-in `examples/synthetic-plugin` or another minimal local fixture suitable for manual testing.

## Non-Scope

- No hosted marketplace browsing, signing, dependency solving, auto-update, Git clone/update, or registry network work.
- No migration of all first-party plugins into separate repos.
- No broad package architecture refactor, new service layer, or package-manager abstraction.
- No SPA, Rails relay, TUI UI, or Project Pipelines plugin surface changes unless a public command contract requires a doc-only note.
- No PII or raw local path output in operator-facing package list/show/status text.

## Assumptions And Unknowns

- Assumption: "install" should mean "persist the local package record in disabled state"; "enable" should be a separate admission/lifecycle action.
- Assumption: `packages enable --path` can remain as an existing convenience path while acceptance tests prove the explicit install then enable flow. If implementation chooses to remove it, update existing tests/docs in the same change and avoid a dual path.
- Assumption: "show/status" can be satisfied by `packages show <name>` if it includes state, classification, version, capability count/details, and diagnostics. It does not need to expose raw source paths.
- Unknown: whether the closed dependency added a distinct lockfile API beyond the existing `PackageRegistrySnapshot`; implementer should inspect current APIs before adding fields.
- Unknown: exact desired event vocabulary. Use existing daemon diagnostics/error rows unless the code already has a package event enum to extend.
- No human question blocks planning. If implementation discovers a required verb cannot be implemented without waiving ticket scope, ask the human rather than silently narrowing.

## Affected Surfaces And Files

- `src/packages.rs`
  - Add `remove` to `PackageRegistry`.
  - Add or reuse package lookup/show helpers.
  - Add `PackageAction::Remove` and ensure errors/decisions carry typed action and state.
  - Preserve compatibility/capability denial behavior through existing admission helpers.
- `src/daemon_transport.rs`
  - Add daemon request handling for install, remove, and show/status.
  - Persist after install/remove/enable/disable through `persist_package_registry`.
  - On enable, keep `load_package_after_enable`; on disable/remove, unload before or after state mutation in a way that leaves durable state consistent if unload reports an expected not-loaded condition.
  - Return fresh package snapshots after every mutation.
- `crates/botster-hub-client/src/lib.rs`
  - Extend `DaemonRequest`, `DaemonPackageDecision`, and possibly `DaemonResponseKind` only as needed.
  - Keep DTO fields sanitized and serde-stable for local clients.
- `src/main.rs`
  - Extend `PackageCommand` parsing for `install`, `remove`, and `show`.
  - Keep output compact, deterministic, and path-neutral.
  - Update usage/error text if present.
- `src/client_api.rs`
  - Likely no mutation support needed; list/show may reuse package projection. Touch only if the production daemon path needs a typed local-client operation.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add a serialized real-daemon integration test covering install/list/show/enable/disable/remove/restart with disposable data dir.
  - Add negative CLI tests for invalid manifest, incompatible package, duplicate id, and capability denial diagnostics.
- `tests/support/mod.rs` or local helpers in `tests/hub_daemon_lifecycle_test.rs`
  - Add fixture writers for invalid manifest, incompatible manifest, duplicate id, and denied capability if local helpers are clearer.
- `examples/synthetic-plugin/`
  - Keep as the manual first-party/local fixture; adjust manifest only if required by current contracts.
- `README.md`
  - Update Local production runtime operator CLI with exact commands for explicit install, show, enable, disable, remove.

## Risks

- Runtime-path risk: adding registry methods without wiring daemon transport would satisfy code-level tests but not the user CLI path. Acceptance must prove `botster-hub packages ...` talks to the running daemon.
- Durable-state risk: remove/disable ordering can leave a package enabled in `hub-state.json` after a lifecycle unload failure. Prefer deterministic decision ordering and test the post-command list/status.
- Load-failure risk: enabling a bad Lua package can persist enabled state before load fails. Implementation should either fail before persistence when possible or emit a structured load-failure diagnostic and leave a documented, test-covered state.
- Sanitization risk: local install paths can leak in CLI output or diagnostics. Operator-facing list/show output should avoid raw package and data-dir paths; tests should assert this for success paths.
- Compatibility drift risk: closed dependency APIs may exist but this worktree could still be stale. Compile before assuming missing APIs.
- Overreach risk: package marketplace terms can invite network registry design. Keep this local path flow only.

## Acceptance Checks And Tests

- `./test.sh --test hub_daemon_lifecycle_test cli_packages_local_path_install_enable_disable_remove_flow`
  - Starts a real daemon with explicit disposable `--data-dir`.
  - Installs `examples/synthetic-plugin` or test fixture by local path as disabled.
  - Lists and shows package state.
  - Enables by package name and proves lifecycle loaded for Lua packages.
  - Disables and proves lifecycle unloaded or inactive.
  - Removes and proves package no longer appears after restart.
  - Asserts success output does not include the package dir or data dir.
- `./test.sh --test hub_daemon_lifecycle_test cli_packages_local_path_diagnostics_are_actionable`
  - Covers invalid manifest, incompatible Botster requirement, duplicate id, and capability denial.
  - Asserts nonzero CLI status where appropriate and structured/actionable diagnostic text or DTO rows.
- Existing package/lifecycle tests should remain green, especially:
  - `cli_packages_enable_local_path_routes_through_running_daemon_and_persists`
  - `cli_packages_enable_without_running_daemon_does_not_mutate_hub_state`
  - Project Pipelines plugin lifecycle tests that use `EnablePackageLocalPath`.
- Optional narrower unit tests in `src/packages.rs` should cover `remove`, duplicate install, and package-not-installed errors if integration failures are hard to localize.
- Manual docs should let a developer run:
  - start daemon
  - install local path
  - show/list
  - enable
  - disable
  - remove
  - shutdown

## Vault Gaps Worth Capturing

- Capture a note if package removal establishes a durable convention for unload-before-delete versus delete-before-unload.
- Capture a note if the implementation settles whether `packages enable --path` remains as a convenience alias or is replaced by explicit install+enable.
- Capture a note if load failure after persisted enable becomes a documented package-state convention.
- Capture a note if Project Pipelines checklist creation continues to time out; this plan used the artifact/gate fallback.

## Convention Review

- No loaded convention conflict found.
- The plan keeps hub package policy in `botster-hub`, manifest/capability contracts in `botster-core`, and the CLI thin over daemon transport.
- The plan follows [[package mutations require the running daemon owner]] and [[serve daemon package reads must refresh registry after mutations]] by requiring mutation through the running daemon and fresh package snapshots after changes.
- The plan follows [[botster package registry persists through hub state json]] and [[botster package records persist trust compatibility and admitted capability lock metadata]] by reusing the existing registry snapshot contract.
- The plan avoids marketplace/network scope and speculative abstractions.
