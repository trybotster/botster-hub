---
title: Expose foreground package app open support for terminal clients
ticket: ticket_1782775851_342888
run: run_1782775871_752067
step: botster_plan
---

# Expose foreground package app open support for terminal clients

## Context Loaded

- Pipeline context: ticket `ticket_1782775851_342888`, run `run_1782775871_752067`, step `botster_plan`, gate `botster_plan_gate`.
- Gate prompt: attach plan evidence for context loaded, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks/tests, and vault gaps.
- No prior artifacts, findings, reviews, questions, or answers were present for this run.
- Playbooks and vault notes loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[installed apps are daemon app rows projected from package runnable entrypoints]], [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[botster runnable entrypoints are hub owned launch contracts]], [[manifest required injections must be consumed by the launched runtime]], [[external client hub tests use subprocess spawned hub test support]], [[test script required for rust tests not cargo test]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Run checklist created: `checklist_1782775934_470204`. Initial creation returned a plugin worker timeout, then `list_checklists` confirmed persistence and all four vault workflow items were updated.

## Scope

- Keep the implementation hub-owned. `botster-tui` should be just a foreground terminal app package entrypoint launched through ordinary app mechanics.
- Preserve the production path already present in the repo: CLI `apps open` -> daemon `ListApps` selector resolution -> daemon `ResolveAppLaunch` -> foreground child process with inherited stdio.
- Extend the public same-device client/test-support surface so terminal-client repos can install a local terminal app package, resolve/open it through hub app mechanics, assert injected hub/package environment, perform a real hub action, and observe clean exit without linking hub internals.
- Add or tighten tests around a local package declaring a `terminal_app` / `foreground_stdio` runnable entrypoint.
- Document any public helper or protocol usage in `docs/client-protocol.md` only if the implementation changes the public contract or adds reusable test-support API that downstream clients are expected to call.

## Non-Scope

- Do not implement this inside `botster-tui`.
- Do not use `StartPackageEntrypoint` background supervision for the terminal path.
- Do not reconstruct manifest command/args/env in terminal clients or tests outside daemon `ResolveAppLaunch`.
- Do not add new package manifest vocabulary, app selector semantics, or optional configurability unless the current public DTOs are insufficient.
- Do not refactor unrelated package lifecycle, app registry, TUI alias, or web-app supervision behavior.

## Botster Layers Touched

- Rust hub daemon/socket protocol: terminal app launch resolution and environment injection if gaps are found.
- Public client boundary: `crates/botster-hub-client` DTO/helper coverage if the app-open path needs a client-facing helper.
- Test-support boundary: `crates/botster-hub-test-support` reusable conformance helper for first-party terminal clients.
- CLI: `src/main.rs` only if `apps open` needs a small correction to preserve foreground app-open behavior.
- Tests/docs: integration tests under `tests/` and public protocol docs if API/test-support behavior changes.

## Affected Surfaces and Files

- `src/main.rs`: current `open_terminal_app` uses `ResolveAppLaunch`, inherits stdio, forwards exit status.
- `src/daemon_transport.rs`: current `ResolveAppLaunch` validates enabled package state, `terminal_app`, `foreground_stdio`, and returns command, args, working directory, and environment.
- `src/packages.rs`: runnable entrypoint manifest validation and environment defaults.
- `crates/botster-hub-client/src/lib.rs`: `DaemonRequest::ListApps`, `DaemonRequest::ResolveAppLaunch`, `DaemonResolvedAppLaunch`, serde tests.
- `crates/botster-hub-client/generated/daemon-protocol.ts` and `src/typescript.rs`: update only if DTO shape changes.
- `crates/botster-hub-test-support/src/lib.rs`: add reusable downstream-shaped foreground terminal app open/conformance helper.
- `tests/hub_daemon_lifecycle_test.rs`: add live daemon regression proving the helper and production path.
- `docs/client-protocol.md`: update only if new public helper semantics need documentation.

## Assumptions and Unknowns

- Assumption: canonical current injection names are `BOTSTER_HUB_SOCKET` and `BOTSTER_HUB_DATA_DIR`; the ticket's `BOTSTER_HUB_CONNECTION` and `BOTSTER_PACKAGE_DATA_DIR` are interpreted as examples or older names unless implementation finds a current canonical equivalent already documented elsewhere.
- Assumption: a controlled local package fixture is valid for proving hub-owned foreground terminal app support, as long as the fixture is launched through daemon app mechanics and performs a real hub action over the injected connection.
- Assumption: "app-open path" for terminal clients means reusable public test-support/API mechanics, not necessarily adding a separate `OpenApp` daemon request, because current architecture separates registry `ListApps` from request-scoped `ResolveAppLaunch`.
- Unknown: whether `botster-hub-client` should grow a convenience helper that runs the process, or whether test support should own process spawning while the client crate remains DTO/request-only. Prefer test support unless implementation proves repeated client code would otherwise recreate daemon launch policy.
- Unknown: whether current `ResolveAppLaunch` injects the package data directory expected by terminal clients. If `BOTSTER_HUB_DATA_DIR` is insufficient, add a daemon-owned package-data env value rather than pushing path inference into terminal clients.
- Worktree/target assumption: this plan applies only to the assigned pipeline worktree for `run_1782775871_752067` and target `tgt_7e208a0c76a44980a83b63af976b1f22`.

## Implementation Plan

1. Audit the exact existing foreground terminal path in `src/main.rs` and `src/daemon_transport.rs`. Confirm no terminal app launch branch invokes `StartPackageEntrypoint` and that child stdio and exit status are preserved.
2. Add a downstream-shaped test-support fixture in `crates/botster-hub-test-support`:
   - write or install a temporary local package with `entrypoints: []` and one `terminal_app` / `foreground_stdio` runnable entrypoint;
   - launch via hub app mechanics by requesting `ListApps` and `ResolveAppLaunch`;
   - run the resolved command with daemon-provided working directory and environment;
   - make the child use injected hub connection details to perform a real daemon action, such as `Status` or `ListApps`, then exit zero;
   - return a stable report with app identity, injected env flags, real hub action result, child stdout/stderr summary, and exit code.
3. If needed, add a small client crate helper for app launch resolution only. Keep process execution in test support unless a public client API is clearly warranted.
4. Add live integration coverage from this repo that starts an isolated hub, runs the test-support terminal app-open helper through public client/test-support APIs, and asserts clean exit plus real hub action evidence.
5. Preserve existing CLI coverage for `botster-hub apps open --data-dir <dir> botster-tui`; update only if implementation discovers it does not receive required env or does not preserve child exit semantics.
6. Regenerate TypeScript protocol only if public DTOs change.

## Risks

- Underwiring risk: adding helper code without proving the production entry point would fail the ticket. Tests must drive `ListApps` and `ResolveAppLaunch` against a running daemon.
- Environment-name drift: tests could assert old example names instead of current canonical variables. The implementation should document and assert canonical names only.
- False positive fixture risk: a package can test env presence without performing a real hub action. The helper must connect to the daemon socket or otherwise use the injected hub connection to request status/list data.
- Boundary creep: adding a daemon `OpenApp` request or client-side launch policy could duplicate the existing `ResolveAppLaunch` contract. Avoid unless the current boundary cannot satisfy test-support needs.
- Process hygiene: subprocess helpers must kill/wait children on failed readiness and avoid inheriting unintended stdin outside the intended foreground execution.

## Acceptance Checks and Tests

- `./test.sh --test hub_daemon_lifecycle_test cli_apps_open_terminal_uses_foreground_launch_contract -- --test-threads=1`
- Add and run a focused live test, expected name similar to:
  `./test.sh --test hub_daemon_lifecycle_test external_client_test_support_exercises_foreground_terminal_app_open -- --test-threads=1`
- Run client/test-support crate tests after adding public helpers:
  `./test.sh -p botster-hub-client -p botster-hub-test-support`
- If DTO or generated protocol changes:
  `./test.sh -p botster-hub-client`
  and update `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Run formatting and lint gates appropriate to touched Rust files: `cargo fmt` or `cargo fmt --check`, and the repo-enforced clippy command used by this project.
- Manual/user-path proof to include in the implementation report: a local terminal app package is installed/enabled, appears in `ListApps`, opens via `botster-hub apps open --data-dir <dir> botster-tui` or equivalent helper path, receives hub/data env, performs a daemon request, and exits with code 0.

## Pipeline Gates and Artifacts

- Plan gate evidence should point to this file and checklist `checklist_1782775934_470204`.
- Implement gate evidence should include committed code, exact verification command output, and the production path proof from CLI/test-support to daemon app mechanics.
- Review should reject code that only adds DTOs or fixture manifests without proving the runtime launch path.

## Vault Gaps Worth Capturing

- Capture after implementation if a stable convention emerges for `botster-hub-test-support` foreground app-open helpers for first-party terminal clients.
- Capture if canonical package data env naming is clarified beyond `BOTSTER_HUB_DATA_DIR` versus package-specific data dir semantics.
- No convention conflict found during planning.
