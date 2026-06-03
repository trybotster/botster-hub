# Hub Capability Grant And Admission Policy Over Core

Ticket: `ticket_1780447078_785690`
Run: `run_1780455775_186277`

## Context Loaded

- Project Pipelines context loaded for the active Plan step: ticket `Implement hub capability grant and admission policy over core`, run `run_1780455775_186277`, current step `botster_plan`, gate `botster_plan_gate`, no prior artifacts, findings, reviews, open questions, or question answers.
- Dependency state: prerequisites `Harden core host-profile authority boundary for plugin management` and `Implement hub package and provider registry over core manifests` are closed.
- Pipeline target: `trybotster/botster-hub`, base `main`, assigned pipeline worktree, target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster planning notes loaded as constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Ticket-specific vault notes loaded: [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster hub is a first party host profile over core]], [[botster core host profile compatibility checks stay deliberately narrow]], [[botster package manifests and lockfiles should declare capabilities and provenance]], [[botster cloud should be an installable privileged provider not a hub dependency]], [[botster core lua owns plugin framework primitives not product policy]], [[cold turkey migrations eliminate dual code paths and version suffixes]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[project pipelines sqlite write locks require preserved verdicts and operator restart]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- Repo context inspected: `README.md`, `docs/adr/hub-as-host-profile-over-core.md`, prior `docs/plans/*`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `src/lib.rs`, `src/profile.rs`, `src/packages.rs`, `src/runtime.rs`, `src/auth.rs`, `src/config.rs`, `src/persistence.rs`, `src/main.rs`, and `tests/hub_runtime_test.rs`.
- Current locked `botster-core` source inspected from Cargo's git checkout at revision `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`: `Capability`, `CapabilitySet`, `CapabilitySurface`, `PackageManifest`, `HostProfileMetadata`, `AdmittedHostProfile`, `HostProfileAdmissionError`, and `admit_host_profile` are public core contracts.
- Current repo state: `PackageRegistry` already installs manifests, stores provenance/pin/update metadata, checks requested capabilities against a hub-owned `CapabilitySet`, rejects host-profile provider packages without metadata, delegates host-profile admission to `botster_core::admit_host_profile`, and has focused unit tests for those behaviors.
- Gap found: `PackageRegistry` currently has no production caller outside public docs/tests, and decision results only cover successful enable/disable plus typed errors. The ticket acceptance asks for hub-owned policy over core, audit-friendly decision results, and proof the runtime/user path changed, so implementation should wire a narrow hub-owned admission/grant entrypoint rather than leave the registry as isolated test scaffolding.
- Checklist discipline: `project_pipelines_create_vault_checklist` was attempted and failed with a Project Pipelines SQLite write lock. Per [[project pipelines sqlite write locks require preserved verdicts and operator restart]] and [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan artifact and gate evidence preserve notes read, convention checks, verification plan, and durable-knowledge decision.

## Scope

In scope:

- Add or adjust hub-owned package admission/grant policy so package enablement is expressed as deterministic decisions over public `botster_core` contracts:
  - `PackageManifest`
  - `Capability`
  - `CapabilitySet`
  - `CapabilitySurface`
  - `admit_host_profile`
  - `HostProfileAdmissionError`
- Preserve core as the enforcement and contract owner. Hub may choose grants, governed surfaces, provenance, audit reasons, and package lifecycle state, but must not bypass or reimplement core host-profile admission.
- Deny unsafe or unknown scope requests by default:
  - requested capability surface not governed by the hub profile is denied,
  - requested capability not present in the hub-owned grant set is denied,
  - provider packages without host-profile metadata are denied,
  - ordinary plugins that try to claim host-profile-only authority are denied through core admission.
- Make admission outcomes audit-friendly and deterministic. Prefer a single decision/result shape for accepted and denied install/enable/disable/pin/admission operations, or a small addition that makes errors record the same package/action/classification/requested-capability/audit-reason context as successful decisions.
- Prove production entrypoint usage. Add a narrow hub-owned facade or runtime-adjacent policy path that calls the registry before provider/plugin enablement would occur. A good shape is a small `HubPackagePolicy`/`PackageAdmissionPolicy` value owned by the hub config/profile layer, or a method on `HubRuntime` only if it remains policy-adjacent and does not mix package decisions with PTY/session mechanics.
- Keep auth hooks as hub policy seams. `src/auth.rs` may be updated only to name package/provider admission hook points if needed; do not implement OAuth, device code, cloud auth, or provider-specific auth.
- Update README/ADR text only where needed to explain the now-wired package admission path and its intentionally in-memory scaffold status.
- Add focused Rust tests that prove accepted and denied package capability requests, deterministic ordering/results, ordinary plugins cannot claim host-profile-only grants, and hub code calls only public `botster_core` APIs.

Botster layers touched:

- Rust hub crate: package/admission policy and public facade.
- Rust core dependency: public API consumer only; no code changes.
- Docs: README/ADR/plan wording if changed surfaces need discovery.
- Not touched: Lua plugins, TUI, React SPA, Rails relay, MCP, product marketplace, external providers.

Worktree/target assumptions:

- Work must stay in the assigned pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- No additional agent spawn is needed for this Plan step.

Pipeline gates and artifacts:

- This file is the repo-visible Plan artifact required by [[plan steps need reviewable plan artifacts]].
- `botster_plan_gate` should attach this same context, scope, assumptions, affected surfaces, risks, acceptance checks, and vault gaps.

## Non-Scope

- No changes to `botster-core`.
- No new package marketplace, package fetcher, git clone/fetch behavior, package index, installer, lockfile format, persistence database, product app store, or auto-update policy.
- No provider implementation, cloud federation, signaling relay, browser shell, SSO, OAuth/device-code flow, secrets provider, or external API integration.
- No plugin lifecycle loader, process supervisor, Lua/WASM runtime, or provider startup ordering beyond the narrow admission/policy seam.
- No new capability vocabulary in `botster-hub`; use core `Capability` and `CapabilitySurface`.
- No broad runtime refactor of `HubRuntime` session/PTY methods.
- No local path dependency override or dependency version churn unless current compilation requires it.
- No PII-bearing audit records. Tests and docs should use synthetic package names, URLs, checksums, and reasons.

## Assumptions And Unknowns

Assumptions:

- The intended "hub capability grant and admission policy" is a hub-side package/provider enablement policy over core manifest/capability/admission contracts, not a new core enforcement primitive.
- `PackageRegistry` is the right starting point because it already owns in-memory install/enable/disable/pin state and uses current core contracts.
- The current in-memory scope is acceptable for this ticket as long as decision results are auditable and the production hub entrypoint uses the policy path. Persistence remains future work under `PersistenceBucket::PackageState`.
- Provider host-profile authority is host-profile-only: ordinary plugin packages may request ordinary capabilities, but if they carry `host_profile` metadata, `admit_host_profile` must deny them as `NotProvider`.
- Denying `CapabilitySurface::Timers` in current tests is valid because the hub profile currently does not govern that surface.
- A narrow policy facade is preferable to putting package admission into generic runtime session methods, because `HubRuntime` should stay a facade over core local engine/session mechanics.

Unknowns for implementer to confirm:

- Whether the final production entrypoint should live in `src/packages.rs` as a higher-level policy type, in `src/profile.rs` as profile-owned grant metadata, or as a narrow public method exported from `src/lib.rs`. Pick the smallest option that proves hub production code uses the policy.
- Whether `PackageDecision` should be extended to cover denied outcomes, or whether `PackageRegistryError` should carry enough audit fields to satisfy "audit-friendly decision results" without collapsing success and error types.
- Whether `auth.rs` needs a small enum variant/name update for admission hooks, or whether existing `AuthHook::ProviderEnablement` and `AuthHook::ClientAdmission` are already enough.
- Whether README/ADR should be updated only with a short "admission policy path" paragraph or left unchanged if code/tests make the path discoverable through public docs.

No human question is blocking at plan time. The ticket acceptance is specific enough if implementation treats current `PackageRegistry` as partial prior work and requires production-path wiring plus audit-result hardening.

## Affected Surfaces / Files

Expected code surfaces:

- `src/packages.rs`
  - Main implementation surface. Extend registry/policy result types and tests; keep using public `botster_core` imports only.
  - Add a deterministic audit/result shape if current `PackageDecision`/`PackageRegistryError` does not preserve enough denied-decision context.
  - Add a narrow production-facing package admission/grant function or type if needed to keep policy separate from runtime PTY mechanics.
- `src/lib.rs`
  - Publicly export any new narrow policy type/result only when production callers or docs need it.
  - Keep doctest aligned with current public package policy entrypoint.
- `src/profile.rs`
  - Possible narrow update if the hub profile should expose default governed surfaces or grant policy metadata. Do not invent hub capability vocabulary.
- `src/auth.rs`
  - Optional hook-name alignment only; auth implementation remains out of scope.
- `src/main.rs`
  - Optional smoke-path proof if the chosen production entrypoint can be exercised without package installation side effects. Do not turn `run-one` into a package manager.
- `README.md` and `docs/adr/hub-as-host-profile-over-core.md`
  - Optional small alignment updates if the policy path becomes concrete enough to document.
- `tests/hub_runtime_test.rs`
  - Only if runtime-adjacent production proof needs integration coverage. Keep session runtime proof intact.
- `Cargo.toml` / `Cargo.lock`
  - Read-only unless current core API usage requires a deliberate lock refresh. Do not introduce a local/path override.

Reference-only surfaces:

- `src/config.rs` and `src/persistence.rs` for existing policy seam language.
- Locked `botster-core` package files for public contract shape.
- Prior plan docs for boundary and dependency policy.

## Risks

- Isolated-scaffold risk: tests can pass while no runtime or hub entrypoint ever uses the admission policy. Mitigation: require a production-facing policy path or explicitly documented scaffold-only rationale; this ticket should prefer wiring.
- Boundary inversion risk: hub could reimplement core host-profile admission or capability contracts. Mitigation: call `botster_core::admit_host_profile` and use `Capability`/`CapabilitySet` directly.
- Overcoupling risk: putting package admission into `HubRuntime` could mix package policy with PTY/session mechanics. Mitigation: prefer a package/profile policy facade unless a runtime method is only a thin caller of that facade.
- Audit weakness risk: current errors are typed but may not record enough operator-friendly context such as audit reason and requested/admitted capabilities. Mitigation: add deterministic decision/error records and tests over exact fields.
- Scope creep risk: package policy can easily turn into marketplace, persistence, update, secrets, or provider runtime implementation. Mitigation: keep this ticket in-memory and admission-only.
- Trust escalation risk: ordinary plugins might gain host-profile-only authority if hub bypasses core admission for plugin packages with `host_profile` metadata. Mitigation: tests must cover plugin-with-host-profile denial.
- Stale docs risk: README/ADR can claim `PackageRegistry` is the policy gate while production code remains unwired. Mitigation: update docs only after the code path exists, and state scaffold limits.
- PII risk: audit examples can accidentally include local paths or identities. Mitigation: use synthetic URLs/checksums/reasons and scan diff.
- SQLite workflow risk: Project Pipelines checklist/gate writes can lock. Mitigation: preserve this plan artifact and submit gate evidence one write at a time.

## Acceptance Checks / Tests

Required verification:

- `./test.sh` passes.
- `cargo fmt --check` passes, or `cargo fmt` is run before final diff.
- `cargo clippy --all-targets --all-features -- -D warnings` passes, or any failure is attributed exactly to unchanged baseline with diagnostics.
- `cargo metadata --format-version 1 --no-deps` confirms `botster-core` remains the git dependency and no local/path override was introduced.

Behavioral acceptance:

- Tests prove a package whose requested capabilities exactly match the hub-owned grant set can be enabled, and the decision records package name, action, state, classification, admitted host-profile data when present, and audit reason or equivalent audit context.
- Tests prove a package requesting an ungranted scope is denied by default with a deterministic denial reason.
- Tests prove a package requesting an ungoverned/unknown surface is denied by default even if that capability appears in the grant set.
- Tests prove provider packages without host-profile metadata are denied before enablement.
- Tests prove provider packages with valid host-profile metadata are admitted through `botster_core::admit_host_profile`.
- Tests prove ordinary plugin packages cannot claim host-profile-only grants; plugin packages carrying `host_profile` metadata must surface core's `HostProfileAdmissionError::NotProvider` or an equivalent wrapped denial.
- Tests prove core host-profile admission denial reasons are preserved/wrapped without stringly typed matching.
- Tests prove admission decisions are deterministic: records are returned in stable package-name order and repeated equivalent inputs produce equivalent decisions/errors.
- Tests prove the new production-facing hub policy path calls the registry/admission logic. Evidence can be a unit/integration test over the public facade, not just direct `PackageRegistry::enable` tests.
- Static scan: `rg -n "ProviderCapability|CapabilityGrant|PackagePolicy|provider capability vocabulary|hub owns the capability vocabulary" src README.md docs tests` returns no reintroduced duplicate hub capability vocabulary except in prior plan docs.
- Static scan: `rg -n "botster_core::package::|botster_core::runtime::capability" src` should not be needed if core root exports remain available; if module paths are used, implementation report must explain why.
- PII scan: `git diff main...HEAD` contains no local absolute paths, secrets, emails, phone numbers, fingerprints, or real tokens in code, docs, tests, or audit fixtures.

Runtime/user-path proof:

- The implementation report must identify the production entrypoint that now uses hub package admission policy. Acceptable proof examples:
  - a new public hub package policy facade constructed from `host_profile()` and used by smoke/boot code,
  - a `HubRuntime`-adjacent method that delegates to package policy without owning package internals,
  - or a documented scaffold-only rationale if implementation determines no runtime package-loading path exists yet. Because this ticket says "implement," a real narrow entrypoint is preferred.

## Vault Gaps Worth Capturing

- Capture candidate if implementation settles a durable rule: "botster-hub package admission policy must have a production-facing facade, not only registry unit tests."
- Capture candidate if audit-result shape becomes a reusable convention: "hub policy denials should carry the same package/action/audit context as successful decisions."
- No capture is needed for the core/hub/provider boundary itself; existing notes already cover hub-as-host-profile, core-owned capability contracts, narrow core admission, and provider packages as installable privileged packages.
