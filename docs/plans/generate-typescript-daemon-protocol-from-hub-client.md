# Generate TypeScript daemon protocol from botster-hub-client

## Context loaded

- Pipeline context: ticket `ticket_1782259481_539120`, run `run_1782259505_165369`, step `botster_plan`, gate `botster_plan_gate`.
- Prior pipeline state: no prior artifacts, open findings, open questions, or prior answers were present when planning.
- Vault/playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo context inspected: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/Cargo.toml`, `Cargo.toml`, `src/daemon_transport.rs`, `docs/client-protocol.md`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`.
- Workflow note: Project Pipelines checklist creation initially returned plugin worker timeouts, but the records persisted and were updated. This plan also preserves the evidence in case checklist writes regress per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Make `crates/botster-hub-client` the source of truth for browser-visible daemon protocol TypeScript.
- Add a deterministic, checked-in TypeScript protocol artifact generated from the Rust serde DTOs in `crates/botster-hub-client`.
- Include request, response, event, package DTO, plugin lifecycle/surface DTO, diagnostics, and coordination/session DTO families.
- Ensure the artifact includes the variants called out by the ticket: `ShowPackage`, `InstallPackageLocalPath`, `EnablePackage`, `DisablePackage`, `RemovePackage`, package entrypoint lifecycle requests, `PluginLifecycleStatus`, `PluginSurfaceRender`, `PluginSurfaceAction`, and `DaemonShutdown`.
- Add a Rust-owned generation/check path, preferably a cargo subcommand/test path in the hub-client crate or workspace, so drift fails without requiring a Node build.
- Add representative serde JSON tests that prove Rust serialization and generated TypeScript coverage stay aligned for all request/response/event families.
- Update protocol docs to say Rust serde remains canonical and browser clients import the generated artifact rather than maintaining handwritten mirrors.

## Non-scope

- Do not add `UpdatePackage`.
- Do not add hub restart semantics.
- Do not change daemon wire behavior unless the generator exposes an existing serde inconsistency that must be fixed to generate the current contract.
- Do not edit an out-of-tree `botster-web` checkout; this repo should publish the artifact that browser clients consume.
- Do not introduce a JavaScript/Node build requirement for this Rust repo.

## Assumptions and unknowns

- Assumption: the generated TypeScript can live in this repo, likely under `crates/botster-hub-client/generated/` or another clearly documented path, and downstream browser clients will import or vendor that checked artifact from the hub revision.
- Assumption: `serde_json::Value` fields should generate as a broad JSON type rather than attempting to encode internal plugin UI schemas.
- Assumption: `PathBuf` request fields can generate as string-like JSON fields, matching serde's browser-visible shape.
- Unknown: whether the implementation should use an existing Rust crate for serde-to-TypeScript export or a small local generator. Because conventions prefer minimal dependencies and framework primitives, first inspect current dependency impact and only add a generator dependency if it materially reduces maintenance risk.
- Unknown: exact artifact path and naming. Pick the narrowest repo-local convention and document it in `docs/client-protocol.md`.

## Botster layers touched

- Rust hub client protocol crate: public daemon DTO source of truth and generator/check tests.
- Rust daemon transport: production entry point should continue re-exporting/using `botster_hub_client` DTOs; only touch if compile errors or drift tests reveal mismatches.
- Browser/client contract docs: document generated artifact consumption.
- No Project Pipelines runtime/plugin behavior is directly changed.

## Affected surfaces/files

- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/Cargo.toml`
- Potential new generated artifact path, for example `crates/botster-hub-client/generated/daemon-protocol.ts`
- Potential new generator/check module, test, example, or small binary under `crates/botster-hub-client`
- `docs/client-protocol.md`
- Possibly `Cargo.toml` / `Cargo.lock` if a Rust generator dependency or workspace command is added
- Existing tests in `crates/botster-hub-client/src/lib.rs` should be extended rather than duplicating broad integration coverage elsewhere

## Risks

- Generator fidelity: serde attributes such as tagged enums, `rename_all = "snake_case"`, defaults, skipped empty fields, `PathBuf`, and `serde_json::Value` must map to the actual JSON wire shape.
- Dead artifact risk: a generated file can drift if the check path only verifies generation exists. Add a test/check that regenerates and compares exact contents.
- Coverage risk: representative tests must include every request/response/event family, not only the variants most recently missing.
- Dependency risk: adding a generator crate may pull in broad dependencies; keep it isolated to dev/build tooling if used.
- Browser usability risk: TypeScript should export stable names and JSON helper types that a browser client can import directly, not just a schema dump.
- Runtime proof risk: because this ticket is intentionally contract/artifact work, production-path proof is that `src/daemon_transport.rs` and live daemon tests continue using/re-exporting `botster_hub_client` DTOs. Do not claim a browser runtime path changed inside this repo unless an in-tree browser consumer is added.

## Acceptance checks/tests

- Run `cargo fmt`.
- Run the focused hub-client test suite, for example `BOTSTER_ENV=test cargo test -p botster-hub-client`.
- Run a drift check that regenerates the TypeScript artifact and fails when checked-in output differs.
- Add/extend raw serde tests proving representative JSON for:
  - `DaemonRequest` variants including package show/install/enable/disable/remove, entrypoint start/stop/restart/status, plugin lifecycle/surface/action, and shutdown.
  - `DaemonResponse` families including status, packages, package decisions, plugin lifecycle, plugin surface/action results, diagnostics, coordination messages, session cleanup, and operator errors.
  - `DaemonEvent` variants including lifecycle, terminal output, snapshot, scrollback, process exit, attach state, and runtime observation.
- Run the relevant existing daemon lifecycle tests if production DTO re-exports or transport mapping are touched, such as `BOTSTER_ENV=test cargo test --test hub_daemon_lifecycle_test`.
- Confirm `docs/client-protocol.md` names the generated artifact and states Rust serde remains canonical.

## Vault gaps worth capturing

- Capture a durable note if implementation settles a reusable Botster convention for generated browser protocol artifacts from Rust serde DTOs.
- Capture a note if the generator choice reveals a repeatable serde-to-TypeScript mapping rule for `serde_json::Value`, `PathBuf`, skipped/default fields, or tagged enums.
- No convention conflict found in planning. The plan follows [[botster hub client crate is the external client boundary]], [[botster web dto field names must match authoritative rust serde structs]], minimal dependency conventions, and the pipeline artifact fallback note for checklist timeout.

## Pipeline checklist evidence fallback

- Context checkpoint: loaded pipeline current context, gate prompt, ticket, run, no prior findings/questions/artifacts.
- Vault checkpoint: loaded required planner and Botster notes listed above; no conflicts identified.
- Repo checkpoint: inspected canonical protocol crate, daemon transport re-export path, current docs, and existing serde/live daemon tests.
- Verification checkpoint for Plan: repo-visible plan artifact added at this path; implementation-stage verification commands are listed above.
- Capture checkpoint: defer durable capture until implementation confirms a reusable generator pattern or drift-check gotcha.
