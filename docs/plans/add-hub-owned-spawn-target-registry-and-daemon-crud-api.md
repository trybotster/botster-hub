# Add hub-owned spawn target registry and daemon CRUD API

## Context Loaded

- Pipeline context: ticket `ticket_1783463498_522016`, run `run_1783463517_301911`, Plan step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, dependencies, or answers were present.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster vault context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[device hub owns admitted spawn targets not ambient repo cwd]], [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster hub client crate is the external client boundary]], and [[generated typescript dtos must encode serde field optionality]].
- Skill context: `botster-customize-hub` because this changes hub commands/API and plugin-facing capability behavior.
- Repo context inspected: `src/persistence.rs`, `src/session_templates.rs`, `src/daemon.rs`, `src/runtime.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/main.rs`, `src/lua_runtime.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, `docs/client-protocol.md`, `docs/lua-plugin-abi.md`, `tests/hub_daemon_lifecycle_test.rs`, and `tests/hub_lua_runtime_test.rs`.
- Pipeline checklist evidence: `project_pipelines_checklist_instructions` loaded. `project_pipelines_create_vault_checklist` timed out in the plugin worker, so the checklist evidence is preserved here and in the gate payload per [[project pipeline orchestration belongs in a device-level botster plugin]] / [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Replace the legacy `HubState.admitted_session_template_targets: Vec<AdmittedSessionTemplateTarget>` source of truth with a hub-owned persisted spawn-target registry in `src/persistence.rs`.
- Add a small hub-owned model/module for spawn targets that owns:
  - stable `target_id`;
  - display name or label;
  - root path;
  - enabled state;
  - generic kind/type, likely a plain string or narrow enum such as `directory`;
  - sanitized metadata for clients/plugins, without PII or local absolute paths in committed fixtures/docs.
- Migrate existing repo-local session-template discovery to read enabled spawn targets from the new registry, preserving current behavior where `.botster/session-templates.json` under an admitted target contributes repo-local templates.
- Expose daemon/client protocol CRUD:
  - list spawn targets;
  - show spawn target;
  - create spawn target;
  - update spawn target;
  - delete spawn target.
- Route daemon CRUD through the existing production owner-thread path: `DaemonRequest` -> `src/daemon_transport.rs` -> `HubClientApi`/hub-owned policy -> `HubRuntime`/state store persistence. Mutations must persist `hub-state.json` and reload through `HubRuntime::load`.
- Add thin CLI commands under a new explicit surface, for example `botster-hub spawn-targets <list|show|create|update|delete> --data-dir <path> ...`, so users and clients can manage targets without editing JSON.
- Add Lua/plugin capability APIs so enabled plugins can list spawn targets and validate a referenced target id without owning the registry. A narrow shape such as `botster.capabilities.spawn_targets.list()` and `botster.capabilities.spawn_targets.validate({ target_id = "..." })` is enough for this ticket.
- Update generated client artifacts when protocol DTOs change:
  - add Rust DTOs in `botster-hub-client`;
  - update conformance fixture revision/features if the protocol feature set changes;
  - regenerate/check in `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Update docs explaining the boundary: hub owns spawn targets; `botster-core` does not; plugins reference stable target ids; plain directories are valid targets and git metadata is optional.

## Non-Scope

- Do not add spawn-target policy to `botster-core`.
- Do not encode Codex, Claude, Project Pipelines, workspace, or git-specific semantics in the registry model.
- Do not add a second compatibility source beside the new registry. The old admitted-session-template target list should be removed or cold-turkey migrated, not kept as a parallel path.
- Do not build a broad workspace manager, target discovery crawler, browser UI, cloud sync, target permissions matrix, or package/provider registry refactor.
- Do not require git for create/admission. A plain existing directory is enough.
- Do not change terminal data-plane behavior, session-worker protocol, package lifecycle policy, or plugin-owned Project Pipelines workflow state except where plugin APIs need to consume spawn targets.

## Assumptions And Unknowns

- Assumption: "stable id" can be caller-supplied or generated by the hub if omitted, but the implementation must persist one stable `target_id` and reject duplicates.
- Assumption: "display name/label" can be represented as one client-facing `label` field unless plan review or product direction requires both `name` and `label`.
- Assumption: target `kind/type` should stay generic. If there is no current use for multiple kinds, `directory` is the initial value and future kinds can be additive.
- Assumption: client-facing metadata should be explicitly sanitized and small. Avoid exposing raw hub-state internals; committed docs/tests should use temp paths or placeholders instead of user home paths.
- Assumption: deletion may remove the target record even if templates referenced it. Existing template resolution should then fail with a typed not-found/not-admitted error instead of silently using stale roots.
- Assumption: disabled targets remain persisted but do not contribute repo-local templates and should fail validation as unusable unless callers ask for existence-only validation.
- Unknown: exact CLI flags for create/update. Proposed minimum: `create --id <target-id> --label <label> --root <path> [--kind directory] [--disabled]`, `update <target-id> [--label ...] [--root ...] [--enable|--disable] [--metadata-json ...]`, `delete <target-id>`.
- Unknown: whether protocol feature constants should add `spawn_targets` and bump `CONFORMANCE_FIXTURE_REVISION` by one. Implementer should follow current `botster-hub-client` convention; a new request/response family likely warrants both.
- No blocking human question is needed because the ticket explicitly allows cold-turkey migration and gives clear ownership boundaries.

## Affected Surfaces And Files

- `src/persistence.rs`: replace `AdmittedSessionTemplateTarget` and `HubState.admitted_session_template_targets` with the new spawn-target registry snapshot; add serde defaults/migration if needed so old state can load into the new field without a parallel runtime source.
- New or existing hub module, likely `src/spawn_targets.rs`: registry/model helpers, create/update/delete validation, id uniqueness, path normalization, enabled filtering, sanitized projection, and persistence snapshot logic.
- `src/lib.rs`: export new hub-owned model/DTO helpers used by tests and daemon code.
- `src/session_templates.rs`: source repo-local templates from enabled spawn targets; keep cwd/template validation under the target root; preserve existing package/device template behavior.
- `src/runtime.rs`: expose state access/mutation helpers if needed, keeping persistence policy hub-owned.
- `src/daemon.rs`: ensure `HubDaemon` loads registry state and exposes mutation helpers consistently with package registry/state persistence.
- `src/client_api.rs`: add `HubClientRequest`, `HubClientOperation`, response body, admission, and projection types for spawn-target list/show/create/update/delete and validation if kept transport-neutral.
- `src/daemon_transport.rs`: add `DaemonRequest` routing, owner-thread mutation handling, persistence after mutations, DTO mapping, operator errors, and response builders.
- `crates/botster-hub-client/src/lib.rs`: add public daemon DTOs/requests/responses, feature descriptor if used, examples/tests, operation labels, serde optionality/defaults, and conformance fixture coverage.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`: regenerate and verify TypeScript DTOs with optional fields matching serde.
- `src/main.rs`: add/parse/render `spawn-targets` CLI commands and help text; keep the CLI thin over daemon requests.
- `src/lua_runtime.rs`: add plugin capability table for spawn-target list/validate, using the loaded plugin's admitted hub capability path rather than raw filesystem access.
- `docs/client-protocol.md`: document new daemon feature/requests and generated artifact expectations.
- `docs/lua-plugin-abi.md`: document Lua spawn-target list/validate helper and clarify that plugins reference hub-owned target ids.
- `README.md`: add user-facing hub-owned spawn target explanation and boundary.
- `tests/hub_daemon_lifecycle_test.rs`: daemon CRUD, persistence reload, non-git directory target, CLI-backed path if existing harness supports it, and repo-local session-template discovery through the new registry.
- `tests/hub_lua_runtime_test.rs` or `tests/hub_plugin_lifecycle_test.rs`: plugin-facing Lua API can list and validate target ids through the real plugin runtime.

## Risks

- Persistence migration risk: keeping both `admitted_session_template_targets` and a new registry would create conflicting truth. Mitigation: cold-turkey runtime source plus a narrow load-time serde migration/default only if old fixtures/state need to deserialize.
- Path leakage risk: spawn target roots are local absolute paths at runtime. Mitigation: avoid committed absolute user paths in docs/fixtures; keep DTO metadata sanitized; tests use temp dirs.
- Protocol drift risk: adding request/response fields without regenerating TypeScript or conformance examples breaks downstream clients. Mitigation: update generated artifact and run the hub-client protocol tests.
- Unwired implementation risk: adding a registry model without routing session templates and daemon/CLI/plugin calls through it would not change the runtime path. Mitigation: real daemon tests must create a target through the public API, restart, and prove repo-local template discovery uses it.
- Git assumption regression: existing repo-template tests use the current checkout path. Mitigation: add a target root temp directory with no `.git` and prove create/list/template discovery works.
- Plugin capability overreach: Lua helpers could expose raw host filesystem or mutation policy. Mitigation: keep plugin API read/validate only for this ticket, and route through hub-owned sanitized projections.
- Admission category risk: current `HubClientAdmission` has broad package/runtime buckets. Mitigation: explicitly categorize spawn-target operations as hub policy/admin operations and document the initial local-operator-only admission.

## Acceptance Checks And Tests

- Rust unit tests for the spawn-target registry model:
  - create/list/show/update/delete;
  - duplicate id rejection;
  - disabled target filtering;
  - root must be an existing directory;
  - non-git directory is accepted.
- Persistence tests in `src/persistence.rs` or adjacent tests:
  - registry persists through `FileHubStateStore`;
  - `HubRuntime::load` or daemon restart reloads targets;
  - old admitted session-template target state is either migrated into the new registry or intentionally removed with a documented cold-turkey fixture update.
- Daemon/API tests in `tests/hub_daemon_lifecycle_test.rs`:
  - CRUD via `DaemonRequest` and `botster_hub::daemon_transport_request`;
  - restart daemon and verify list/show still return the target;
  - create a plain temp directory with `.botster/session-templates.json` and no `.git`, then prove `ListSessionTemplates`/`SpawnSessionTemplate` resolve through the new registry;
  - disabled/deleted target no longer contributes templates and validation reports a typed operator error.
- CLI tests or command-level assertions:
  - `botster-hub spawn-targets create/list/show/update/delete --data-dir <tmp>` drives daemon-backed CRUD, not direct JSON edits.
- Lua/plugin API tests:
  - an enabled Lua plugin lists spawn targets;
  - the same plugin validates an existing enabled target id;
  - validation for missing or disabled target id returns a structured false/error result without filesystem access.
- Protocol/generation checks:
  - hub-client request/response serde examples include spawn-target DTOs;
  - TypeScript generated artifact contains `DaemonSpawnTarget` and CRUD request/response shapes with serde-accurate optional fields;
  - update `CONFORMANCE_FIXTURE_REVISION`/feature list if current convention requires it.
- Docs checks:
  - `docs/client-protocol.md`, `docs/lua-plugin-abi.md`, and `README.md` explain hub/core/plugin boundaries and no-git directory support.
- Final verification commands:
  - `./test.sh --test hub_daemon_lifecycle_test <spawn-target filters>`
  - `./test.sh --test hub_lua_runtime_test <spawn-target/plugin filters>` or `./test.sh --test hub_plugin_lifecycle_test <spawn-target/plugin filters>`
  - `cargo test -p botster-hub-client`
  - `cargo fmt`
  - `cargo clippy --all-targets --all-features -- -D warnings`

## Vault Gaps Worth Capturing

- If implementation settles the model shape, capture a durable note such as "hub spawn target registry is the canonical admitted target source" with the final DTO fields and migration rule.
- If plugin list/validate semantics become durable, capture whether disabled target validation means "exists but unavailable" or simply false.
- If a new protocol feature is added, capture the convention for when daemon CRUD additions require feature constants and conformance fixture revision bumps.
- The checklist worker timeout should be captured if it recurs outside this run; for this plan the fallback evidence is preserved in artifact/gate payloads.

