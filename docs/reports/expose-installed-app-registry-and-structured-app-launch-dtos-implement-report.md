---
ticket: ticket_1782361545_680661
run: run_1782364556_708985
step: botster_implement
---

# Implement Report

## Files changed

- `Cargo.lock`: updated `botster-core` and `botster-core-daemon` to the core revision that exposes `RunnableEntrypoint*` DTOs.
- `crates/botster-hub-client/src/lib.rs`, `src/typescript.rs`, and `generated/daemon-protocol.ts`: added `ListApps`, `DaemonApp`, `DaemonAppLaunchTarget`, and `apps` response DTOs.
- `src/packages.rs`, `src/client_api.rs`, `src/persistence.rs`, `src/lib.rs`, `src/main.rs`: reconciled runnable entrypoint DTOs to core `web_app`/`terminal_app` and `background`/`foreground_stdio` vocabulary.
- `src/daemon_transport.rs`: added production `ListApps` projection from package registry plus `EntrypointSupervisor` snapshots.
- `src/entrypoint_supervisor.rs`: carries core `RunnableEntrypointLaunchResult` and refreshes it from a supervised runtime's structured launch-result file.
- `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_plugin_lifecycle_test.rs`: updated fixtures and added live app-registry coverage.
- `docs/client-protocol.md`: documented installed app registry DTOs, launch target semantics, and no diagnostics/stdout/env URL parsing.
- `docs/plans/expose-installed-app-registry-and-structured-app-launch-dtos.md`: durable approved plan artifact.

## Playbook constraints applied

- Loaded `[[implementer-playbook]]` and `[[botster-implementer-playbook]]`.
- Kept scope to the approved plan plus returned review findings.
- Used core `RunnableEntrypoint*` vocabulary cold-turkey instead of preserving hub-local kind/mode vocabulary.
- Exposed app registry state through the daemon/client DTO boundary; no friendly CLI app commands were added.
- Proved the production path: `DaemonRequest::ListApps` reads installed package runnable entrypoints plus supervisor launch snapshots and returns `DaemonResponse.apps`.
- Used `./test.sh` for Rust tests where applicable.
- Persisted this durable report artifact for review.

## Deviations from plan

- Added a minimal hub-owned structured producer path for launch results after review found that `local_url` was readable but never produced. The supervisor injects `BOTSTER_ENTRYPOINT_LAUNCH_RESULT`; supervised runtimes can write a serialized core `RunnableEntrypointLaunchResult` there. The app projection still exposes `local_url` only from `RunnableEntrypointLaunchResult.local_url` when readiness declares `LocalUrl`.
- Changed `launch_target.kind` to mirror core app kind labels (`web_app`, `terminal_app`) rather than the initially implemented `web`/`terminal` labels, resolving the returned low review finding and avoiding a second vocabulary.

## Tests run

- `cargo update -p botster-core -p botster-core-daemon` passed; updated both core pins from `2eafcee` to `42538009`.
- `cargo check -p botster-hub-client` passed.
- `cargo check` passed.
- `cargo fmt` passed.
- `git diff --check` passed.
- `./test.sh -p botster-hub-client` passed: 23 unit tests and 4 doctests.
- `./test.sh --test hub_client_api_test` passed: 12 tests.
- `./test.sh --test hub_daemon_lifecycle_test daemon_list_apps_projects_installed_package_entrypoints` passed and proves child-emitted `RunnableEntrypointLaunchResult.local_url` reaches `ListApps` as `launch_target.local_url`.
- `./test.sh --test hub_daemon_lifecycle_test -- --test-threads=1` passed: 50 tests.
- `cargo clippy --all-targets --all-features -- -D warnings` passed.

## Unverified behavior or residual risk

- The launch-result file path is a local hub-supervision contract; malformed or missing JSON is ignored until a valid core `RunnableEntrypointLaunchResult` exists.
- No URL parsing, known-port inference, diagnostics parsing, or client inference was added.
- `app_id` remains the core runnable entrypoint id, namespaced by `package_name`; no separate globally unique app id was introduced.

## Missing vault guidance discovered

- Captured a new vault inbox note: [[botster structured launch-result output fields need producer paths]].
- The reusable rule: structured output DTOs need a real producer path, not just a gated read-side projection, or they must be explicitly documented and tested as scaffold.
