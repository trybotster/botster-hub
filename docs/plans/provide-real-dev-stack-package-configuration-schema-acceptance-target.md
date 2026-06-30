# Provide Real Dev-Stack Package Configuration Schema Acceptance Target

## Context Loaded

- Project Pipelines current context loaded for ticket `ticket_1782796365_386145`, run `run_1782838502_822103`, step `botster_plan`, gate `botster_plan_gate`.
- Ticket: provide hub/dev-stack support for a real local first-party package that exposes an authoritative package configuration schema and deterministic validation behavior suitable for botster-web acceptance.
- No prior artifacts, findings, reviews, questions, answers, or open dependencies were present in the run context.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster/vault context loaded: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], and [[test script required for rust tests not cargo test]].
- Repo context inspected: `src/main.rs`, `src/packages.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `README.md`, `docs/client-protocol.md`, `examples/synthetic-plugin/botster-package.json`, and prior dev-stack/package-configuration plans.
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` returned `plugin worker invoke timeout`, so this plan and the gate evidence carry the checklist evidence fallback per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add a real first-party dev-stack package configuration acceptance target that downstream botster-web acceptance can use without local-only browser fixtures.
- Prefer the smallest path: extend the existing dev-stack acceptance fixture package set with a checked-in or test-generated first-party-style package whose manifest declares a deterministic `configuration` schema.
- Ensure the package is installable/enabled through the `botster-hub dev-stack bootstrap` path or the same daemon-owned local package path used by dev-stack tests.
- Prove `ListPackages` returns `DaemonPackage.configuration.schema` from the authoritative Rust DTO path.
- Prove `SetPackageConfiguration` accepts a valid save, rejects a known invalid value with hub-returned diagnostics, and a subsequent `ListPackages` returns refreshed persisted configuration state.
- Update README or client protocol docs only if a new named dev-stack package/path/acceptance command becomes part of the supported operator contract.

## Non-Scope

- No botster-web local fixture changes; this ticket exists to remove reliance on browser-only fixtures.
- No new daemon protocol request unless current `SetPackageConfiguration`/`ListPackages` cannot satisfy the acceptance path.
- No schema vocabulary expansion beyond the current core-owned package configuration schema contract.
- No broad dev-stack refactor, new package lifecycle abstraction, Project Pipelines workflow change, or UI workbench work.
- No secret raw-value round trip. Existing redaction semantics must remain intact.

## Assumptions And Unknowns

- Assumption: "real local first-party package" means a package participating in the hub/dev-stack first-party package path, not the existing ad hoc `configurable.plugin` temporary test package.
- Assumption: downstream botster-web acceptance needs a stable package name and deterministic fields/invalid value, so the implementation should name those explicitly in test/docs.
- Assumption: the current public DTOs are sufficient: `DaemonPackageConfiguration.schema`, `effective_values`, `missing_required`, and `diagnostics`.
- Unknown: whether the best target is a new checked-in example package such as `examples/configurable-first-party` or an extension of an existing first-party fixture in `tests/hub_daemon_lifecycle_test.rs`. Prefer checked-in only if botster-web needs a stable package path outside Rust tests.
- Unknown: whether dev-stack bootstrap should enable this package by default for daily operators. If adding it to default dev-stack would force required config before enablement, ask a human rather than silently weakening validation.

## Botster Layers Touched

- Rust hub CLI/dev-stack path: `dev-stack bootstrap` package installation and printed operator contract if the package becomes a named dev-stack input.
- Rust daemon/package runtime: package registry validation, persistence, and sanitized daemon package DTO projection.
- Hub client protocol DTOs: `DaemonPackage.configuration` remains the authoritative client-facing schema/effective-state contract.
- Tests: real daemon lifecycle acceptance through the same socket request path botster-web uses.
- Browser SPA is the intended consumer but should not be modified here.

## Affected Surfaces And Files

- `tests/hub_daemon_lifecycle_test.rs`: add or extend a focused real-daemon acceptance test proving the dev-stack package configuration target through install/enable/list/set/reject/list/restart or persisted refresh.
- `examples/.../botster-package.json`: possible new checked-in first-party-style package fixture if a stable external package path is needed for botster-web acceptance.
- `src/main.rs`: only if `dev-stack bootstrap` must accept or install the new package target as an official first-party dev-stack input.
- `README.md` and/or `docs/client-protocol.md`: only if a new supported dev-stack acceptance package/command is introduced.
- `src/packages.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`: should remain unchanged unless implementation finds the existing production path fails the ticket's deterministic diagnostics or refreshed-state requirements.

## Runtime Proof Requirements

- The proof must use the running daemon owner path, not direct registry mutation:
  - install/enable through `DaemonRequest::InstallPackageLocalPath`, `EnablePackage`, or `dev-stack bootstrap`;
  - read through `DaemonRequest::ListPackages`;
  - mutate through `DaemonRequest::SetPackageConfiguration`;
  - verify the response and a fresh later `ListPackages`.
- The invalid-value assertion must inspect hub-returned operator diagnostics, not only a failed process exit.
- The refreshed-state assertion must prove persisted/effective state after the save, ideally with a second daemon read and, if cheap, a daemon restart reload.
- The package row must expose `DaemonPackage.configuration.schema` with stable field keys and value shapes botster-web can consume.

## Risks

- Fixture risk: extending the existing synthetic `configurable.plugin` test would satisfy mechanics but not the "dev-stack first-party" acceptance target. The package name/path must make the first-party dev-stack intent explicit.
- Enablement risk: a required-field schema can block package enablement during dev-stack bootstrap. If the acceptance package must be enabled by default, it should either have safe defaults or be installed-but-disabled intentionally and documented.
- Diagnostics risk: tests that only check `OperatorError` miss whether botster-web can display useful validation details. Assertions should cover diagnostic kind/message shape.
- Contract drift risk: TypeScript/browser consumers must follow Rust serde DTOs; do not introduce fixture-only field names such as `config_schema`.
- Path/PII risk: dev-stack output and plan/report artifacts should not expose local home paths.

## Acceptance Checks And Tests

- Focused command:
  - `./test.sh --test hub_daemon_lifecycle_test <new_or_updated_dev_stack_package_configuration_acceptance_test> -- --test-threads=1`
- Nearby regression checks:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_bootstrap_starts_daemon_enables_first_party_packages_and_prints_apps -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test package_configuration_daemon_set_show_list_reload_and_cli_are_redacted -- --test-threads=1`
- Run `cargo fmt` if Rust files change.
- If DTO structs or generated TypeScript change unexpectedly, run the hub-client protocol generation/drift test used by this repo before review.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this file and summarize the loaded context, scope/non-scope, assumptions/unknowns, affected files, risks, tests, and vault gaps.
- Implement should attach a report identifying the stable package name/path, exact configuration fields, valid payload, invalid payload, returned diagnostics, and refreshed state evidence.
- Review should reject code-only evidence that does not prove the running daemon/dev-stack path.
- Verify should rerun the focused acceptance test or inspect exact command evidence and confirm no browser-local fixture dependency remains.

## Convention Conflicts

None found. The plan follows the loaded Botster constraints: hub owns package/runtime policy, public clients consume authoritative Rust daemon DTOs, dev-stack remains a local first-party package path, workflow policy stays out of core, and test evidence uses the repo wrapper rather than direct `cargo test`.

## Vault Gaps Worth Capturing

- Capture a durable note if this ticket establishes a named convention for "first-party configurable package acceptance target" in dev-stack.
- Capture a note if implementation discovers that default dev-stack enablement and required package configuration schemas need a standing rule.
- No new vault note is needed at Plan time for checklist timeout fallback, plan artifact discipline, package DTO authority, or Rust test wrapper usage; existing notes cover those constraints.
