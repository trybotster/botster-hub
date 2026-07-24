# Durable Hub State Model And Storage Boundary Plan

Ticket: `ticket_1780508731_136973`
Run: `run_1780510185_724219`

## Context Loaded

- Project Pipelines context loaded for current Plan step `botster_plan`, run step `run_step_1780510185_128348`, gate `botster_plan_gate`, and ticket `Define durable hub state model and storage boundary`.
- No prior artifacts, findings, reviews, open questions, question answers, or blocking dependencies were present in the run context.
- Required playbooks loaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Botster planning notes loaded:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[project pipelines needs an operator workbench not more primitives]]
  - [[project pipelines ui contract belongs in the plugin readme]]
  - [[botster orchestration should spawn agents with explicit target ids]]
  - [[botster orchestration prompts must bind agents to explicit worktrees]]
- Identity/goals vault context loaded from [[identity]] and [[goals]].
- Repo context inspected:
  - `src/persistence.rs` is currently a placeholder bucket enum with no storage API.
  - `src/packages.rs` has the concrete in-memory package registry, provider classification, enable/disable/pin state, provenance, and last audit reason.
  - `src/config.rs` defines host identity, startup config, runtime data directory resolution, transport bindings, session defaults, and core-engine runtime settings.
  - `src/runtime.rs` is the production hub facade over `botster_core::DefaultBotsterEngine`; it currently stores only `HubConfig` plus the core engine.
  - `src/lib.rs` exposes public hub facade types, package registry types, config types, and the `persistence` module.
  - `src/main.rs` has the binary smoke path and `run-one` production entrypoint over `HubRuntime::new(config)`.
  - `README.md` already names the persistence seam as hub-owned and explicitly says concrete persistence databases are not implemented yet.
  - `tests/hub_runtime_test.rs` proves runtime facade behavior through core but does not cover durable hub state.
  - Existing plan docs show the repo convention of committing Plan artifacts under `docs/plans/`.
- Project Pipelines checklist discipline:
  - `project_pipelines_checklist_instructions` was loaded.
  - `project_pipelines_create_vault_checklist` was attempted twice for this run and timed out at the plugin worker boundary.
  - Per [[project pipeline orchestration belongs in a device-level botster plugin]], checklist timeout evidence is preserved in this plan artifact and should also be preserved in the gate payload.

## Scope

In scope:

- Define typed hub state records under the existing hub-owned persistence seam for:
  - host identity metadata,
  - hub config schema/version metadata,
  - package/provider registry records,
  - capability grants and admission decisions,
  - enabled/disabled/pinned package state,
  - audit-friendly decision history,
  - local runtime settings derived from current hub config surfaces.
- Define a stable storage trait/API that can load, save, and update the hub state without exposing file-format details to package policy or runtime callers.
- Add one simple local-first concrete implementation: a deterministic JSON file store under the resolved hub data directory.
- Keep the file format versioned and migration-ready. Version `1` should be explicit; unknown future versions and corrupt JSON should fail with typed errors instead of silently resetting state.
- Use atomic write behavior or an equivalent consistency boundary: write to a sibling temporary file, flush it, and rename it into place.
- Extend package registry state only as needed to make registry records serializable and restorable across process restarts.
- Wire the production runtime path so `HubRuntime` can be constructed with loaded durable state, or provide an explicit `HubRuntime::load`/`with_state_store` entrypoint that the binary smoke path can exercise. Evidence must prove runtime construction uses the storage boundary, not merely that unused types exist.
- Document schema/versioning/migration posture in repo docs near the persistence implementation, plus a short README pointer if the public scaffold exclusions change.
- Add focused tests covering create/load/update, registry/grants persistence across a fresh store instance, corrupt file handling, unknown-version handling, and atomic write behavior or the chosen equivalent.
- Keep fixtures, docs, and test data scrubbed of PII, local usernames, home paths, fingerprints, real hosts, tokens, or emails.

Non-scope:

- No Rails implementation.
- No cloud sync.
- No marketplace fetch, git clone, package download, or lockfile resolution.
- No WebRTC, browser/TUI UI, ActionCable, or API provider work.
- No SQLite or database layer unless JSON cannot satisfy the atomicity and migration requirements; prefer simple local file-backed storage.
- No speculative multi-profile sync, remote account identity, or secrets storage.
- No broad refactor of package policy, runtime facade, or config validation beyond what persistence wiring requires.
- No compatibility shim for old durable files because this repo currently has no durable hub state file to migrate from.

Botster layers touched:

- Rust hub crate: primary.
- Docs: persistence/schema posture.
- Rust core: referenced through existing package/config/runtime contracts only; no changes.
- CLI binary smoke path: narrow production-path proof if `src/main.rs` loads state during boot or `run-one`.
- Plugins/providers, SPA, TUI, Rails, MCP, cloud: non-scope.

Worktree/target assumptions:

- This Plan step is operating in the assigned pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- No additional agent spawning is needed during Plan.

Pipeline gates and artifacts:

- This file is the Plan artifact for review.
- `botster_plan_gate` evidence should attach the same scope, assumptions, affected surfaces, risks, acceptance checks, and vault gaps listed here.

## Assumptions And Unknowns

Assumptions:

- "Durable state boundary" means a production-shaped API and local implementation now, not a final cloud-sync architecture.
- JSON file storage is sufficient for local production runtime because the ticket asks for simple local-first storage and explicit versioning/migration posture.
- Version `1` can be cold-start only: no old file migration is needed, but the code must make future migrations possible by dispatching on schema version.
- `PackageRegistry` remains the policy owner for install/enable/disable/pin validation; persistence stores accepted records and audit history without bypassing registry policy.
- Capability grants and admission decisions should use current `botster_core` capability/manifest/admission types where possible instead of introducing a parallel capability vocabulary.
- Local runtime settings are the durable subset of existing `HubConfig`/`HubStartupOptions` that operators would expect to survive restarts, not every transient session or PTY runtime detail.
- Audit history can be an append-only in-file vector for v1. It does not need a query engine, retention policy, or external log sink in this ticket.

Unknowns for implementer to resolve with code inspection:

- Whether all `botster_core` manifest/admission types used inside `PackageRecord` implement `Serialize`/`Deserialize`. If not, define a hub-owned serializable snapshot record instead of adding custom ad hoc string parsing.
- Whether `PackageRegistry` should expose `into_records`/`from_records` helpers or whether persistence should operate on a new `HubState` aggregate and rebuild the registry through existing policy methods.
- Whether `HubRuntime::new(config)` should stay as the in-memory constructor and a new fallible constructor should load durable state, or whether `new` should remain pure while `src/main.rs` explicitly loads state before constructing runtime.
- Whether tests can validate atomic writes by checking temp-file cleanup and successful old-state preservation on injected write failure, or whether the implementation should use a small injected filesystem trait for deterministic failure tests.

No human question is currently blocking. If implementation would need to ignore the required production-path wiring or replace JSON with a heavier database, stop and ask rather than choosing silently.

## Affected Surfaces / Files

Expected changes:

- `src/persistence.rs`
  - Replace the bucket-only scaffold with typed state records, storage trait/API, file-backed implementation, version dispatch, typed errors, atomic write helper, and unit tests.
- `src/packages.rs`
  - Derive or add conversions for serializable package registry records if needed.
  - Add import/export helpers for durable registry state if persistence should not reach into private fields.
- `src/config.rs`
  - Reuse existing `HostIdentity`, `HubConfig`, and runtime settings types where serialization already exists.
  - Add only narrow snapshot types if the runtime config should not be persisted wholesale.
- `src/runtime.rs`
  - Add a production-facing constructor or accessor that accepts loaded durable state or a state store.
  - Keep `DefaultBotsterEngine` mechanics untouched.
- `src/lib.rs`
  - Re-export new public storage/state API types needed by callers and docs.
- `src/main.rs`
  - Narrowly wire the binary boot or `run-one` path to initialize/load the state store from the resolved data directory, if this is the clearest runtime proof.
- `tests/hub_runtime_test.rs` or new focused tests under `tests/`
  - Add restart/persistence coverage if the behavior crosses public runtime APIs.
- `README.md`
  - Update scaffold exclusions and persistence section if concrete file-backed storage is added.
- `docs/adr/` or `docs/`
  - Add a short persistence/schema posture doc if module docs are not enough.

Reference-only surfaces:

- `Cargo.toml` and `Cargo.lock`
  - Avoid new dependencies. If a dev-only tempdir crate is considered, first prefer `std` plus `target/` or `/tmp` test directories.
- `test.sh`
  - Existing approved test wrapper is `BOTSTER_ENV=test cargo test "$@"`.

## Risks

- Unwired implementation risk: adding durable structs and tests is not enough. The runtime or binary entrypoint must call the storage boundary, or the implementer must document why the ticket is scaffold-only. This ticket's wording expects actual local persistence.
- Policy bypass risk: loading records directly into a registry could skip capability/admission validation. Import helpers should distinguish trusted persisted state from new enable/install decisions and preserve auditability.
- Serialization drift risk: persisting raw upstream `botster_core` types may couple hub state to core internals. If core types are not explicitly stable for storage, snapshot hub-owned serializable records around the fields the hub owns.
- Migration ambiguity risk: accepting unknown versions by default would corrupt future upgrades. Unknown version must produce a typed error, with a documented future migration hook.
- Atomicity overclaim risk: rename-only is not a full crash-consistency proof on every filesystem. State the actual consistency boundary and test the behavior the implementation claims.
- PII risk: host identity, fingerprints, local paths, and audit reasons can easily leak real operator data into docs or fixtures. Tests should use synthetic ids and relative/temp paths.
- Scope creep risk: cloud sync, lockfiles, marketplace fetching, UI, and provider lifecycle are natural next steps but explicitly excluded here.
- Dependency drift risk: this crate depends on `botster-core` from `main`; implementer should compile against the resolved lock revision before committing to serialization choices.

## Acceptance Checks / Tests

Required checks after implementation:

- `cargo fmt`
- `./test.sh`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`

Functional acceptance:

- Typed hub state model exists and includes host identity metadata, schema/config version metadata, package/provider registry records, capability grants/admission decisions, enabled/disabled/pinned state, audit decision history, and local runtime settings.
- A storage trait/API exists with clear load/save/update behavior and typed errors.
- A local deterministic file-backed implementation persists state under the resolved hub data directory or an explicit test path.
- Registry records and grants persist across a fresh process/store instance in tests.
- Create, load, update, and reload paths are tested.
- Corrupt JSON/file content returns a typed corrupt-state error and does not silently reset to defaults.
- Unknown schema version returns a typed unsupported-version error.
- Atomic write or equivalent consistency behavior is implemented and tested according to the claimed boundary.
- Schema/versioning/migration posture is documented in repo docs or module docs.
- Runtime proof identifies the production entrypoint that uses the storage boundary, preferably `src/main.rs` boot or `run-one` plus `HubRuntime` construction.
- Tests and docs contain no PII, local absolute user paths, real fingerprints, secrets, hostnames, emails, or tokens.

Suggested focused test names:

- `file_store_creates_and_loads_default_v1_state`
- `file_store_persists_package_registry_and_capability_grants_across_reopen`
- `file_store_updates_state_atomically`
- `file_store_rejects_corrupt_state_file`
- `file_store_rejects_unknown_schema_version`
- `runtime_boot_loads_hub_state_from_configured_data_directory`

Runtime/user-path proof:

- If `src/main.rs` boot loads the store, add a smoke test or unit seam proving the state file path is derived from `HubConfig.data_directory`.
- If the production binary remains intentionally non-persistent, the implementer must document that as scaffold-only and ask for a human decision because the ticket acceptance explicitly asks for persistence across restarts.

## Vault Gaps Worth Capturing

- Capture candidate after implementation: a durable note for "botster-hub durable state is local file-backed v1 with explicit migration dispatch and later cloud sync/lockfile extension points." This is more specific than the existing architecture notes.
- Capture candidate if discovered: whether `botster_core` package/admission types are storage-stable or whether hub-owned snapshot records are the correct persistence boundary.
- No convention conflict found. The plan aligns with local Botster boundaries: hub owns product state/policy over core contracts; core runtime mechanics remain untouched; no plugin/UI/Rails/cloud work is introduced.
- Checklist persistence gap observed: Project Pipelines checklist creation timed out twice at the plugin worker boundary. Evidence has been preserved in this plan and should be preserved in gate evidence rather than blocking the Plan step.
