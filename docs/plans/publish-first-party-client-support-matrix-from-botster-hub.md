# Publish First-Party Client Support Matrix From Botster Hub

## Context Loaded

- Project Pipelines context: ticket `ticket_1781049175_510399`, run `run_1781049181_452352`, active step `botster_plan`, gate `botster_plan_gate`, no prior artifacts, findings, reviews, questions, or answers.
- Vault/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], [[botster hub client crate is the external client boundary]], and [[external client hub tests use subprocess spawned hub test support]].
- Repo context inspected: `Cargo.toml`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/src/lib.rs`, `crates/botster-hub-test-support/Cargo.toml`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_client_api_test.rs`, `docs/client-protocol.md`, and the prior `docs/plans/publish-external-client-conformance-fixtures.md`.
- Current baseline: `botster-hub-client` owns protocol constants, compatibility descriptors, request/response/event DTOs, diagnostics, and `stream_attach`. `botster-hub-test-support` already owns isolated hub subprocess fixtures and deterministic conformance report helpers. `docs/client-protocol.md` documents the downstream dependency shape and live-hub harness.
- Checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` was attempted for this run and timed out with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and should also be attached to the Plan gate evidence.

## Scope

- Add a small hub-owned, machine-readable first-party client support matrix in `botster-hub-test-support`, close to the conformance helpers that downstream client tests already consume.
- Source protocol fields from `botster-hub-client` constants and compatibility descriptors instead of duplicating literal protocol truth.
- Describe the current first-party support surface in structured fields:
  - supported protocol name, protocol version, and conformance fixture revision;
  - diagnostic kinds clients can branch on today;
  - session actions covered by the public daemon protocol and conformance flow;
  - terminal streaming behavior, including held-open `stream_attach` and stable output assertions;
  - resize support;
  - plugin surface render/action support through JSON daemon responses;
  - entity/action limitations and other intentionally unsupported behavior.
- Add tests proving the matrix aligns with `DaemonCompatibility::current()`, `DaemonCompatibilityRequirement::current()`, and the existing `run_client_conformance` / `run_project_pipelines_conformance` report capabilities.
- Update downstream-maintainer docs in `docs/client-protocol.md` to identify the matrix API, who should consume it, and what remains unsupported.

## Non-Scope

- Do not edit `botster-tui` or `botster-web`.
- Do not add private client-specific policy or renderer instructions to the hub.
- Do not create a prose-only matrix when a structured fixture/API is practical.
- Do not define a second protocol or mirror private daemon-to-session-worker frames.
- Do not add optional configurability, broad abstractions, new transport routes, or runtime client admission policy.
- Do not mutate real Botster identity, host state, or non-test data directories.

## Botster Layers Touched

- Rust hub test-support crate: primary support matrix and downstream test API surface.
- Rust hub client protocol crate: source of compatibility constants and diagnostic/request DTO names; likely import-only.
- Hub daemon lifecycle and session/client-worker runtime: exercised through existing subprocess conformance tests, not reimplemented.
- Docs: downstream client protocol and support matrix guidance.
- No SPA, Rails relay, TUI, or Lua plugin runtime changes.

## Assumptions And Unknowns

- Assumption: the support matrix is a test/docs contract, not a new daemon runtime endpoint; placing it in `botster-hub-test-support` keeps production protocol narrow while giving first-party clients a structured fixture for tests.
- Assumption: `botster-tui` and `botster-web` will consume the matrix from their own tests/docs later, not as part of this ticket.
- Assumption: current "entity/action support" means public plugin surface render/action JSON dispatch, not full plugin-owned entity frame hydration. Full client entity-store conformance should be listed as intentionally unsupported unless the repo already exposes a public hub-client fixture for it.
- Unknown: whether downstream consumers need JSON serialization or typed Rust constants only. Prefer typed Rust structs with a stable public function first; add direct `serde` only if Implementation needs serialized JSON as part of the machine-readable contract.
- Worktree/target assumption: work happens only in the pipeline-assigned worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- No human question is blocking Plan: the ticket intent is specific enough if the matrix is scoped to hub-owned test support and docs.

## Affected Surfaces And Files

- `crates/botster-hub-test-support/src/lib.rs`: add a public support matrix type/function near conformance report types; add unit tests for matrix-to-compatibility alignment where possible.
- `tests/hub_daemon_lifecycle_test.rs`: extend the existing `external_hub_test_support_drives_isolated_daemon_socket_protocol` coverage or add a nearby targeted test proving the matrix matches live conformance report capabilities.
- `docs/client-protocol.md`: document the matrix API, expected downstream consumption, first-party support today, and unsupported/limited areas.
- `crates/botster-hub-test-support/Cargo.toml`: change only if a direct dependency is required for the public machine-readable shape.
- `docs/plans/publish-first-party-client-support-matrix-from-botster-hub.md`: this plan artifact.

## Risks

- Protocol drift: matrix literals can diverge from `DaemonCompatibility::current()`. Mitigation: build protocol fields from `botster-hub-client` constants and assert equality in tests.
- Overstated client support: naming entity/action support too broadly could imply full SPA/TUI entity-store conformance. Mitigation: list only plugin surface/action JSON dispatch as supported and explicitly mark richer entity-frame support as unsupported unless proven.
- Runtime bloat: putting client policy in `botster-hub-client` could make the protocol crate carry docs policy. Mitigation: keep the matrix in test support unless implementation proves runtime clients need it.
- Flaky live tests: subprocess/PTY conformance can race. Mitigation: reuse the existing isolated hub test and stable report booleans instead of adding new timing-sensitive terminal assertions.
- PII/path leakage: docs or diagnostics could expose local data dirs or identity. Mitigation: keep support matrix values path-neutral and synthetic; docs should avoid local home paths.

## Acceptance Checks And Tests

- Add a targeted test proving the support matrix protocol name, version, feature list, and conformance fixture revision match `DaemonCompatibility::current()` and `DaemonCompatibilityRequirement::current()`.
- Add or extend live conformance coverage proving every matrix-supported session/terminal/resize/plugin-surface capability is backed by `run_client_conformance` or `run_project_pipelines_conformance`.
- Verify docs identify the machine-readable API that downstream `botster-tui` and `botster-web` tests/docs should consume, and explicitly call out unsupported limitations.
- Suggested commands:
  - `./test.sh -p botster-hub-test-support`
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol`
  - If rustdoc examples are added: `BOTSTER_ENV=test cargo test -p botster-hub-test-support --doc`
- Verification must prove the actual user path: downstream test code imports `botster-hub-test-support`, reads the support matrix, starts an isolated hub subprocess, and compares the matrix against live `botster_hub_client` compatibility/conformance behavior.

## Pipeline Gates And Artifacts

- Plan gate evidence should attach this plan path and the checklist fallback evidence.
- Plan Review should reject any implementation that:
  - only adds prose without a structured fixture/API;
  - duplicates protocol constants without alignment tests;
  - reaches into private session-worker frames or full hub internals from the matrix;
  - edits `botster-tui` or `botster-web`;
  - claims support not proven by compatibility descriptors or conformance reports.

## Vault Gaps Worth Capturing

- Capture after implementation if the support matrix shape becomes durable guidance: first-party client support matrices belong in hub-owned test support and must be tested against compatibility descriptors plus conformance reports.
- Capture after implementation if a stable vocabulary emerges for differentiating plugin surface/action support from full plugin entity-frame support.
- No durable vault note is needed at Plan time for the checklist timeout; [[project pipelines checklist worker timeouts require artifact evidence fallback]] already covers the fallback.

## Checklist Evidence Fallback

- Vault/context evidence: notes listed in `Context Loaded` constrained the plan to public hub-client boundaries, subprocess test support, path-neutral artifacts, explicit worktree/target assumptions, and `./test.sh` verification.
- Convention-conflict evidence: none found. The ticket aligns with [[botster hub client crate is the external client boundary]] and [[external client hub tests use subprocess spawned hub test support]].
- Verification evidence gathered during planning: repo inspection confirmed compatibility descriptors and conformance helpers already exist and are used by live daemon tests; no implementation tests were run during Plan.
- Capture evidence: no durable knowledge was captured yet; capture is deferred until Implementation proves the final matrix API.
