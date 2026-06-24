---
ticket: ticket_1782257625_477566
title: Persist and expose package configuration values in botster-hub
run: run_1782260163_843763
step: botster_plan
---

# Persist and expose package configuration values in botster-hub

## Context Loaded

- Pipeline context: ticket `ticket_1782257625_477566`, run `run_1782260163_843763`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, or answers. Blocking dependency `ticket_1782257611_241250` is marked closed.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster/vault constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` loaded. Creating the run vault checklist timed out in the plugin worker, so per the known fallback pattern the checklist evidence is preserved in this plan and should be copied into gate evidence.
- Repo context: `src/packages.rs` owns package install/enable policy, `PackageRecord`, `PackageRegistrySnapshot`, local manifest parsing, and persisted registry reload. `src/persistence.rs` persists `HubState.package_registry` in schema-versioned `hub-state.json`. `src/daemon_transport.rs` routes package mutations through the running daemon owner and maps hub packages to daemon DTOs. `src/client_api.rs` exposes sanitized `HubClientPackage` rows. `crates/botster-hub-client/src/lib.rs` owns public daemon request/response DTOs. `src/main.rs` owns thin CLI package commands/output. Existing package tests live in `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `src/packages.rs`/`src/persistence.rs` unit tests.
- Dependency context: `Cargo.lock` currently pins `botster-core` at `1548c0cbc4c93a92c44c7ed1f0018ddfb75592b5`; `git ls-remote` shows upstream `botster-core` `main` at `5150780f3be051d8daf39b6a59cf194530eccd4a`. The implementer should update `botster-core`/`botster-core-daemon` as needed before consuming the closed core package configuration schema.

## Scope

- Consume the core-owned package configuration schema contract from `botster-core` after updating the dependency if the current lock lacks it.
- Add hub-owned policy and storage for package configuration values inside the existing package registry state, keyed by package identity.
- Validate submitted values against the manifest schema before persisting or enabling a package.
- Apply manifest defaults into the effective non-secret configuration presented to clients.
- Model secret fields as redacted/write-only: accept writes through the set/update path, persist only a redacted marker or metadata needed to know a value was supplied, and never expose raw secret values through client, daemon, CLI, debug formatting, or persisted non-secret config.
- Surface missing required config diagnostics from install/enable without executing plugin code.
- Add daemon and CLI surfaces to inspect schema, read redacted effective config, and set/update config.
- Extend package list/show DTOs to include configuration metadata needed by clients: schema presence/fields, required/missing state, defaults/effective redacted values, and diagnostics.
- Prove the production user path through the running daemon owner, not only direct struct/unit tests.

## Non-Scope

- No web or TUI rendering.
- No hosted marketplace, package index, installer, dependency solving, signing, or cloud provider flow.
- No plugin execution as part of config validation; missing required config must be reported before plugin load.
- No general secret store implementation beyond the ticket-required redacted/write-only model.
- No broad package registry rewrite, persistence schema migration framework, or unrelated package entrypoint/lifecycle cleanup.
- No product workflow policy in core Lua or Project Pipelines plugin code.

## Assumptions And Unknowns

- Assumption: the closed core dependency provides serde-stable manifest schema types or validation helpers that hub should reuse rather than duplicating schema semantics.
- Assumption: "persist non-secret config in hub state under package identity" maps to extending `PackageRecord`/`PackageRegistrySnapshot`, which already persist through `HubState.package_registry`.
- Assumption: secret values are not recoverable through this ticket's read APIs. If runtime plugin execution later needs raw secret material, that should be a separate secret-provider/credential-store ticket.
- Assumption: defaults are schema defaults and should appear in redacted effective config even when the operator has not explicitly set the field.
- Assumption: unknown field rejection applies to submitted config keys, not to unrelated existing manifest fields.
- Unknown: exact upstream core type names and validation API until the dependency is updated. The implementer should inspect core after `cargo update -p botster-core -p botster-core-daemon` or equivalent and adapt to those names.
- Unknown: CLI command spelling. Prefer the smallest extension of existing `packages` commands, for example `packages config-schema`, `packages config`, and `packages config set`, unless the existing parser shape points to a clearer local convention.
- Unknown: whether the daemon request should patch one key at a time or accept a JSON object. Prefer an object payload if that matches core validation and lets tests cover unknown field rejection/default merging in one request.

## Botster Layers Touched

- Rust hub package/registry policy.
- Rust hub persistence.
- Rust local daemon transport and public client DTOs.
- Thin CLI package commands.
- Docs/examples/tests.

No Lua plugin, TUI, React SPA, Rails relay, MCP workflow policy, or marketplace layer is required.

## Affected Surfaces/Files

- `Cargo.lock`: update `botster-core`/`botster-core-daemon` if the current lock predates the schema contract.
- `src/packages.rs`: add config value/redaction/effective-config models to package records; validate submitted values, defaults, required fields, unknown fields, and secret redaction against core schema; include missing-required diagnostics in package admission/install/enable paths before lifecycle loading.
- `src/persistence.rs`: prove `HubState.package_registry` persists non-secret config and redacted secret markers across reload without raw secret leakage.
- `src/daemon_transport.rs`: add daemon requests for schema/read/set or route them through existing package request handling; ensure package mutations happen on the daemon owner and persist before responses are returned.
- `src/client_api.rs`: add sanitized package config metadata/effective config to `HubClientPackage` or adjacent client response types.
- `crates/botster-hub-client/src/lib.rs`: add serde-stable daemon DTOs and request variants for config schema/read/set, redacted effective config, package diagnostics, and package list/show metadata.
- `src/main.rs`: extend package CLI parsing/output for schema/read/set while keeping output path-neutral and secret-safe.
- `examples/synthetic-plugin/botster-package.json` or a new fixture: add package configuration schema examples covering required, defaulted, unknown, and secret fields.
- `docs/client-protocol.md` and possibly `README.md`: document daemon/CLI package config surfaces, redaction semantics, defaults, and no web/TUI rendering.
- Tests: `src/packages.rs` unit tests, `src/persistence.rs` persistence tests, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `crates/botster-hub-client` serde tests.

## Implementation Shape

1. Update and inspect `botster-core` package schema types, then import the core schema contract into hub package policy.
2. Extend `PackageRecord` with a serde-defaulted config state that separates non-secret stored values from secret supplied/redacted markers. Keep old persisted package records loadable.
3. Add registry methods for setting/updating config by package name and for computing effective redacted config from manifest schema plus stored values.
4. Gate `install_local_path`/`enable` so missing required config reports a package diagnostic/decision error before `load_package_after_enable` can execute plugin code.
5. Add daemon/client DTOs and request variants for config schema, redacted effective config, and set/update config. Project package list/show rows from the same effective-config helper so list/show and explicit config read cannot drift.
6. Wire CLI package subcommands to those daemon requests. Keep CLI output compact, deterministic, and scrubbed.
7. Add docs and fixtures only for the new public contract.

## Risks

- Underwired implementation risk: adding state and validation helpers without routing through `DaemonRequest`/CLI would not satisfy the ticket. Acceptance must prove a running daemon exposes and persists the behavior.
- Secret leakage risk: raw secret values could leak through JSON state, daemon DTOs, CLI output, diagnostics, debug derives, failed assertion messages, or docs fixtures. Tests should search the relevant outputs/state for secret sentinel strings.
- Dependency drift risk: planning against the old locked core revision could duplicate core schema semantics. Implementation must update and consume the merged core contract first.
- Enable-order risk: existing `EnablePackage` loads plugins after persisting. Missing-required config must fail before `load_package_after_enable`.
- Backward compatibility risk: existing `hub-state.json` package records need serde defaults so package registry reload does not break old state files.
- DTO compatibility risk: public `botster-hub-client` serde fixtures need defaults or explicit revision updates if new fields are added to package rows.
- Scope creep risk: web/TUI rendering, hosted marketplace flows, and a real secret vault are tempting adjacent work but outside this ticket.

## Acceptance Checks/Tests

- Focused package policy tests cover validation success/failure, missing required values, unknown field rejection, default value application, secret redaction/write-only behavior, and package enable rejection before plugin code execution.
- Persistence tests prove non-secret config and redacted secret markers survive `HubState` reload, and raw secret sentinel values do not appear in `hub-state.json`.
- Client API tests prove sanitized package rows include configuration metadata/effective redacted config and never expose raw secrets.
- Daemon lifecycle tests prove a real running daemon can install/show/list a package with config schema metadata, set/update config, reject bad config, persist across restart, and reject enable when required config is missing.
- `botster-hub-client` serde tests cover new daemon request/response DTOs and package rows with configuration metadata.
- CLI tests or daemon-backed integration assertions prove `botster-hub packages ... --data-dir` can inspect schema, read redacted effective config, and set/update values without exposing raw secrets.
- Run `cargo fmt`.
- Run `./test.sh` targeted filters for the changed package/client/daemon surfaces, then a broader `./test.sh` if runtime allows.
- Run strict clippy if the repo gate expects it; any baseline failures must be attributed to touched vs untouched files.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Gate evidence should include context loaded, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Implement gate should require committed code plus evidence that the running daemon/CLI path changed, not only static code presence.
- Review/verify should scan committed artifacts and test output for local paths, PII, and raw secret sentinel leakage.

## Worktree And Target Assumptions

- Run target: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Work happens in the pipeline-assigned ticket worktree for this run, not an ambient checkout.
- Base ref is `main`; dependency ticket is marked closed, but the local lock may still need updating to the latest core main revision.

## Vault Gaps Worth Capturing

- Capture the settled hub representation for package config state: non-secret stored values, secret supplied/redacted markers, defaults, and diagnostics.
- Capture whether package config validation belongs entirely in hub package policy while core owns only schema contracts.
- Capture the final CLI command vocabulary for package config inspection/update if it becomes a durable public contract.
- Capture any recurring Project Pipelines checklist worker timeout only if it blocks more than this known fallback pattern.
