# Hub As Host Profile Architecture Plan

Ticket: `ticket_1780376606_123665`

## Context Loaded

- Project Pipelines context loaded for run `run_1780416227_172808`, current step `botster_plan`, gate `botster_plan_gate`, ticket `Define hub-as-host-profile architecture over botster-core`, and dependency on the closed core-load ticket.
- No prior artifacts, findings, reviews, open questions, or question answers were present for this run.
- Required Plan playbooks loaded:
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
- Ticket-specific boundary notes loaded:
  - [[plan steps need reviewable plan artifacts]]
  - [[project pipelines sqlite write locks require preserved verdicts and operator restart]]
  - [[botster packages should enforce core hub cli plugin provider boundaries]]
  - [[botster core lua owns plugin framework primitives not product policy]]
  - [[botster cloud should be an installable privileged provider not a hub dependency]]
- Repo context inspected:
  - `README.md` already frames `botster-hub` as the product host around `botster-core` and lists a responsibility split.
  - `src/lib.rs` exposes `ArchitectureSummary`, role labels, provider capabilities, and `HubRuntime`.
  - `src/runtime.rs` currently embeds `botster_core::DefaultBotsterEngine` behind `HubRuntime`.
  - `src/config.rs` defines hub startup/config policy, plugin/provider directories, transport bindings, and core-engine knobs.
  - `src/core.rs`, `src/auth.rs`, `src/persistence.rs`, `src/packages.rs`, and `src/providers.rs` are shallow boundary seams.
  - `src/adapters/*` names host adapter seams for clients, cloud, signaling, and API providers.
  - `tests/hub_runtime_test.rs` proves the runtime facade can spawn, attach, write, drain, classify, and shut down through core.
  - `docs/plans/embed-default-botster-engine-hub-runtime.md` is a prior plan for a runtime-embedding ticket; useful context, but not current scope.

## Scope

In scope:

- Add one reviewable ADR/doc that defines the accepted hub-as-host-profile architecture over `botster-core`.
- Treat `botster-core` as the source of truth for reusable command/plugin/runtime mechanisms, while documenting `botster-hub` as a first-party host profile and plugin/provider bundle over those mechanisms.
- Define three boundary tiers:
  - non-replaceable core mechanisms,
  - trusted host-profile privileges,
  - ordinary user-installed plugin/provider capabilities.
- Cover startup ownership: core engine construction, host profile bootstrap, provider/plugin loading order, and where policy is allowed to run.
- Cover ownership of config, persistence, auth/admission, providers, marketplace/package policy, transport/signaling/client policy, plugin lifecycle, and audit hooks.
- Recommend a migration path for `botster-hub` that preserves the existing shallow scaffold while moving toward LazyVim-style first-party profile composition.
- Update `README.md` or existing crate docs only if needed to point to the ADR and keep public discovery aligned.
- Keep all docs free of PII and local absolute worktree paths.

Non-scope:

- No runtime implementation beyond docs or tiny skeleton/doc-link changes.
- No new auth, provider, marketplace, transport, plugin, browser, TUI, Rails, ActionCable, WebRTC, cloud, or persistence implementation.
- No broad refactor of `HubRuntime`, `HubStartupOptions`, provider capability enums, adapters, tests, or package seams.
- No attempt to freeze final public APIs or physically split crates/packages in this ticket.
- No speculative configurability, compatibility shims, or dual-path migration code.
- No treating the existing hub scaffold as gospel; it is evidence and a naming starting point, not the architectural authority.

Botster layers touched:

- Docs: primary layer.
- Rust hub crate: optional doc-link or architecture-summary wording only.
- Rust `botster-core`: referenced as upstream/source-of-truth only; no code changes.
- Plugins/providers: described as architectural boundaries only; no implementation.
- CLI/TUI/SPA/Rails/MCP: referenced only where ownership boundaries require it.

Worktree/target assumptions:

- This plan is for the assigned pipeline worktree, not the implementation target's main checkout.
- The run is bound to target `tgt_7e208a0c76a44980a83b63af976b1f22`; no additional agent spawning is needed during Plan.

Pipeline gates and artifacts:

- This file is the Plan artifact required by [[plan steps need reviewable plan artifacts]].
- The `botster_plan_gate` evidence should attach the same scope, assumptions, affected surfaces, risks, acceptance checks, and vault gaps listed here.

## Assumptions And Unknowns

Assumptions:

- The requested architecture doc is intentionally scaffold/docs-only; the Implementer should not extend runtime behavior unless a narrow doc-link requires it.
- "LazyVim-style" means first-party curated host-profile composition over a reusable core, not a thick wrapper that forks or hides core mechanisms.
- The existing `botster-hub` module names are useful evidence, but the ADR can correct or supersede their implied boundaries when vault notes and `botster-core` surfaces say otherwise.
- Providers are privileged packages when they affect trust, admission, reachability, pairing, registry publication, secrets, remote network access, or browser shell delivery.
- Ordinary plugins should compose core/plugin framework primitives without receiving host-profile privileges by default.
- `botster-core` should remain the non-replaceable owner of reusable engine, session, PTY/process, client/session data-plane, plugin framework primitive, entity/UI contract, package manifest, crypto/identity mechanism, and transport-neutral contract surfaces.

Unknowns:

- Exact current `botster-core` command/plugin/runtime APIs may drift because this repo depends on the `main` branch; implementation should inspect the locked sources or public docs before making concrete claims.
- The final ADR path is open. Prefer `docs/adr/hub-as-host-profile-over-core.md` if an ADR directory is introduced; otherwise use a clear `docs/hub-as-host-profile-over-core.md`.
- Whether `README.md` should be updated with only a link or with a small responsibility-summary revision depends on the final ADR placement.
- Whether package manifests and marketplace policy are currently core-owned, hub-owned, or split in `botster-core` needs verification from code before the ADR states final mechanics.

No human question is currently blocking. If code inspection shows `botster-core` contradicts the three-tier model, stop and ask a human rather than waiving the ticket's requested framing.

## Affected Surfaces / Files

Expected changes:

- `docs/adr/hub-as-host-profile-over-core.md` or `docs/hub-as-host-profile-over-core.md`
  - New ADR/doc with status, context, decision, boundary table, startup ownership, policy ownership, risks, and migration path.
- `README.md`
  - Optional narrow link to the ADR and adjusted wording if current responsibility text conflicts with the accepted architecture.
- `src/lib.rs`
  - Optional narrow doc-comment or `ArchitectureSummary` wording adjustment if README/ADR discovery needs compile-checked alignment.

Reference-only surfaces to inspect during implementation:

- `src/runtime.rs`
- `src/core.rs`
- `src/config.rs`
- `src/persistence.rs`
- `src/auth.rs`
- `src/packages.rs`
- `src/providers.rs`
- `src/adapters/*`
- `tests/hub_runtime_test.rs`
- `Cargo.toml` and `Cargo.lock`

Not expected:

- New Rust modules.
- New dependencies.
- New or changed integration tests beyond doc/link assertions if implementation changes public docs surfaced through code.
- Any Rails, TUI, SPA, Lua plugin, MCP, cloud/provider, marketplace, auth, persistence, or transport implementation files.

## Recommended ADR Shape

Use a single ADR/doc with these sections:

1. Status: proposed/accepted for this ticket.
2. Context: Botster is a Lua plugin platform and local Rust runtime; `botster-hub` should be a first-party host profile over `botster-core`.
3. Decision: hub-as-host-profile/plugin bundle, not a thick wrapper.
4. Boundary table:
   - Non-replaceable core mechanisms.
   - Trusted host-profile privileges.
   - Ordinary plugin/provider capabilities.
5. Startup ownership:
   - core mechanism initialization,
   - host profile bootstrap,
   - privileged provider enablement,
   - ordinary plugin load,
   - client/session attach path.
6. Policy ownership:
   - config,
   - persistence,
   - auth/admission,
   - providers,
   - marketplace/packages,
   - transport/signaling/client contracts,
   - plugin lifecycle,
   - audit/observability.
7. Risks:
   - thick-wrapper drift,
   - provider privilege escalation,
   - core product-policy leakage,
   - dual-path migrations,
   - stale scaffold wording.
8. Migration path:
   - keep current shallow crate as host-profile scaffold,
   - document boundaries first,
   - align README/public docs,
   - audit current seams against ADR,
   - move policy into host profile/provider packages without widening core,
   - make package/capability boundaries enforceable only after docs and current surfaces agree.

## Risks

- Over-implementation risk: this ticket's acceptance is a doc/ADR; adding runtime behavior would create review burden and scope creep.
- Boundary inversion risk: placing provider/cloud/auth/marketplace policy in `botster-core` contradicts the local architecture notes.
- Thick-wrapper risk: making `botster-hub` hide or fork core APIs would fight the requested host-profile model.
- Privilege ambiguity risk: providers need host-enforced capability grants, while ordinary plugins should not silently gain bootstrap, auth, transport, or secret authority.
- Stale scaffold risk: current module names can bias the ADR toward today's shallow scaffold instead of the accepted architecture.
- Dependency drift risk: `botster-core` is referenced from branch `main`; concrete API claims should be verified against the locked dependency before final wording.
- PII risk: plan and ADR docs must not include local absolute paths, usernames, hostnames, or secrets.

## Acceptance Checks / Tests

Required checks after implementation:

- `cargo fmt`
- `./test.sh`
- `cargo test`
- `cargo clippy --all-targets --all-features -- -D warnings`

Documentation acceptance:

- A new ADR/doc exists and is reviewable in the repo.
- The ADR explicitly states `botster-hub` is a first-party host profile/plugin bundle over `botster-core`, not a thick wrapper.
- The ADR defines non-replaceable core mechanisms.
- The ADR defines trusted host-profile privileges.
- The ADR defines ordinary user-installed plugin/provider capabilities.
- The ADR covers startup ownership.
- The ADR covers config, persistence, auth/admission, provider, marketplace/package, transport/signaling/client, plugin lifecycle, and audit policy ownership.
- The ADR lists risks and a recommended migration path for `botster-hub`.
- Any README or crate-doc changes link to or align with the ADR without duplicating the whole decision.
- The final diff contains no PII and no unrelated implementation.

Runtime/user-path proof:

- This ticket is intentionally docs/scaffold-only.
- The production path affected is developer/operator architecture discovery: repo docs plus optional README/crate-doc entrypoint.
- If README or crate docs change, prove the public documentation path points to the ADR. If only the ADR changes, document that no runtime path was intended.

## Vault Gaps Worth Capturing

- Potential gap: no single vault note yet captures "hub as first-party host profile/plugin bundle over botster-core" in this exact LazyVim-style framing. If the ADR settles that wording, capture it as durable Botster architecture knowledge after implementation.
- No convention conflict found so far. The plan aligns with core/hub/provider boundary notes, plugin policy placement, and the Plan artifact requirement.
- Checklist persistence note: initial checklist creation may hit Project Pipelines SQLite lock contention. If checklist writes remain unavailable, preserve checklist evidence in gate payload and this plan artifact.
