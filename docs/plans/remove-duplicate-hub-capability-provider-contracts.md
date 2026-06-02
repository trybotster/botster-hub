# Remove duplicate hub capability and placeholder provider contracts

Ticket: `ticket_1780438401_913631`
Run: `run_1780439873_633502`

## Context loaded

- Project Pipelines context loaded for the active Plan step: ticket `Remove duplicate hub capability and placeholder provider contracts`, run `run_1780439873_633502`, current step `botster_plan`, gate `botster_plan_gate`, no prior artifacts, findings, questions, or answers.
- Dependency state: the prerequisite ticket `Align botster-hub dependency policy with current botster-core` is closed.
- Pipeline target: `trybotster/botster-hub`, base `main`, assigned ticket worktree.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster notes loaded as planning constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster hub is a first party host profile over core]], [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster cloud should be an installable privileged provider not a hub dependency]], [[botster core lua owns plugin framework primitives not product policy]], [[cold turkey migrations eliminate dual code paths and version suffixes]], and [[plan steps need reviewable plan artifacts]].
- Repo context inspected: `src/lib.rs`, `src/packages.rs`, `src/providers.rs`, `src/core.rs`, `src/adapters/{cloud,signaling,api}.rs`, `src/main.rs`, `README.md`, `Cargo.toml`, and `test.sh`.
- Current `botster-core` source exports `Capability`, `CapabilitySurface`, `CapabilitySet`, and `PackageManifest`; the hub should consume those current core contracts instead of keeping parallel provider capability/grant shapes.
- Checklist discipline: `project_pipelines_create_vault_checklist` was attempted for this run and timed out at the plugin worker boundary. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence carry the vault-note provenance, convention-conflict scan, verification plan, and durable-knowledge decision.

## Scope

- Remove hub-only duplicate capability vocabulary where it competes with current core package/capability contracts:
  - Delete `ProviderCapability` as a hub-owned enum, or replace all live package-policy references with `botster_core::Capability` / `botster_core::CapabilitySurface`.
  - Delete `CapabilityGrant` if it remains only a wrapper around the duplicate hub enum and has no production caller; if a grant seam is still useful, make it carry the core capability type and keep only hub-owned policy metadata such as the grant reason.
- Remove dead placeholder provider/core contract enums with no real behavior:
  - `EmbeddedCoreRole`
  - `CloudContract`
  - `SignalingContract`
  - `ApiContract`
- Update public facade and smoke output so the crate no longer advertises “provider capability contracts” as hub-owned vocabulary. The runtime-ready path should still go through `HubRuntime::new` and `architecture_summary()`, but its summary should describe roles/responsibilities and current core-backed package contracts rather than duplicate provider capabilities.
- Update README/module docs so they say `botster-core` owns package manifests, `Capability`, `CapabilitySurface`, host-profile admission contracts, and capability runtime primitives; `botster-hub` owns product host policy over those core contracts.
- Keep any hub-owned provider policy seam only if it expresses admission/lifecycle/product policy over core contracts, not a placeholder implementation contract.
- Add or adjust focused tests so the public facade proves the duplicate vocabulary is gone and the runtime smoke path still compiles through current core.

## Non-scope

- No new provider implementation, cloud implementation, signaling relay, browser shell, API client, marketplace fetcher, package installer, plugin runtime, or persistence database.
- No changes to `botster-core`.
- No local path overrides, dependency-policy changes, or lockfile refresh unless compilation requires it.
- No physical multi-crate split.
- No broad docs rewrite beyond README/module docs touched by the removed vocabulary.
- No new capability taxonomy invented in `botster-hub`.

## Assumptions and unknowns

- Assumption: because the named placeholder enums have no callers beyond docs/tests/smoke text, they can be deleted cold turkey without a compatibility bridge.
- Assumption: `PackagePolicy` can remain as hub-owned policy vocabulary if it does not duplicate core package manifest or capability types.
- Assumption: a small hub-side struct that pairs a `botster_core::Capability` with a hub policy reason is acceptable only if it has a real caller or test-proven rationale.
- Unknown for implementer to confirm before editing: whether deleting `src/providers.rs`, `src/core.rs`, and the placeholder adapter modules leaves any module export or doctest references behind.
- Unknown for implementer to confirm with compile/test evidence: whether `botster_core::Capability` is exported through both root and module paths in the dependency as expected by the current lock revision.

No human question is needed at plan time: the ticket names the duplicate and placeholder types explicitly, and the repo scan shows they are scaffold-only.

## Affected surfaces/files

- `src/lib.rs`: public facade, `ArchitectureSummary`, doctest, tests, and responsibility wording.
- `src/main.rs`: smoke-path summary text and any count based on duplicate provider capability contracts.
- `src/packages.rs`: remove or replace `CapabilityGrant`/`ProviderCapability` with core package/capability contracts.
- `src/providers.rs`: likely delete if it only defines `ProviderCapability`.
- `src/core.rs`: likely delete if it only defines `EmbeddedCoreRole`.
- `src/adapters/cloud.rs`, `src/adapters/signaling.rs`, `src/adapters/api.rs`, and possibly `src/adapters/mod.rs`: delete or reduce to real adapter namespaces only if callers remain.
- `README.md`: responsibility split, crate layout, scaffold exclusions, and dependency policy wording that currently implies hub-owned provider capability vocabulary.
- Tests/doctests in `src/lib.rs` and `tests/hub_runtime_test.rs` only as needed to preserve runtime-path proof and facade expectations.

## Risks

- Public facade churn: removing exported modules/types can break doctests or downstream expectations, but this scaffold has no known external release path; cold-turkey removal matches the ticket and local migration convention.
- Overcorrection: deleting every provider seam could erase legitimate hub policy ownership. Keep policy concepts such as admission/lifecycle/audit only when they are expressed over core contracts and have a caller or test.
- Weak runtime proof: removing docs-as-code can make tests pass while the runtime smoke path no longer reflects production entry. Keep `src/main.rs` and `tests/hub_runtime_test.rs` wired through `HubRuntime` and current `DefaultBotsterEngine`.
- Core API path mismatch: using the wrong `botster_core` import path could produce unnecessary wrapper code. Implementer should inspect the resolved core exports and compile with current lockfile before adding indirection.
- Stale docs: README and module docs can continue implying hub owns the core capability vocabulary after code deletion. Sweep rendered/docs text for the old terms.

## Acceptance checks/tests

- `rg -n "ProviderCapability|CapabilityGrant|EmbeddedCoreRole|CloudContract|SignalingContract|ApiContract" src README.md tests` returns no stale duplicate/placeholder vocabulary unless a remaining occurrence has a real caller and explicit rationale in the implementation report.
- `rg -n "provider capability contracts|hub owns the capability vocabulary|provider capability vocabulary" src README.md tests` returns no stale docs-as-code wording.
- `cargo metadata --format-version 1 --no-deps` confirms `botster-hub` still depends on the intended current `botster-core` source and does not introduce a local/path override.
- `./test.sh` passes and proves the hub crate, doctests, and `tests/hub_runtime_test.rs` still compile/run through the current core runtime path.
- `cargo clippy --all-targets -- -D warnings` passes, or any failure is tied to pre-existing unrelated baseline with exact diagnostic evidence.
- Production entry proof: `src/main.rs` still constructs `HubRuntime::new(config)` and reads `architecture_summary()` without duplicate provider capability counts; `tests/hub_runtime_test.rs` still proves spawn, attach, write, drain, classify, and shutdown through `HubRuntime`.
- Committed diff/PII check: `git diff main...HEAD` contains no local absolute paths, secrets, emails, phone numbers, or other PII.

## Vault gaps worth capturing

- No new vault note is required at plan time. Existing notes already cover the core-vs-hub/provider boundary, host-profile framing, core-owned package manifests/capabilities, and checklist timeout fallback.
- Capture candidate only if implementation discovers a more specific rule, such as “botster-hub package policy structs may wrap core capabilities only with real hub policy metadata and callers.”
