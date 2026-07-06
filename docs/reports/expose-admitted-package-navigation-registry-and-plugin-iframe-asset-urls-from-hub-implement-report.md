---
ticket: ticket_1783371372_931094
run: run_1783374681_864878
step: botster_implement
---

# Implement Report

## Files changed

- `Cargo.lock`: refreshed `botster-core` and `botster-core-daemon` to the revision that exposes `PackageManifest.navigation` and `UiNodeKind::Iframe`.
- `crates/botster-hub-client/src/lib.rs`, `src/typescript.rs`, and `generated/daemon-protocol.ts`: added `ListPackageNavigation`, `DaemonPackageNavigationEntry`, `DaemonPackageNavigationSource`, the `package_navigation` response field, and generated TypeScript coverage.
- `src/client_api.rs`: added internal `ListPackageNavigation` projection, explicit manifest navigation mapping, and default app-surface fallback for packages without explicit navigation.
- `src/daemon_transport.rs`: wired the daemon request, route-backed navigation rows, enabled/blocked diagnostics, missing-target diagnostics, and response serialization.
- `src/lib.rs`, `src/local_webrtc.rs`, `src/main.rs`: exported the new DTOs, updated response literals, and added CLI print support for the new response kind.
- `src/packages.rs`, `src/persistence.rs`, `tests/hub_plugin_lifecycle_test.rs`: updated direct `PackageManifest` fixtures for the refreshed core `navigation` field.
- `tests/hub_client_api_test.rs`: added explicit-navigation and default-app-surface navigation tests, including no order/priority authority.
- `tests/hub_daemon_lifecycle_test.rs`: added live daemon coverage for `ListPackageNavigation`, disabled/enabled route diagnostics, route parity, and typed iframe `PluginSurfaceRender` responses with no raw HTML fields.
- `packages/hub-test-support/daemon-protocol.ts` and `metadata.json`: synced published client test-support assets.
- `docs/client-protocol.md`: documented the navigation registry contract and the iframe URL/reference scope.
- `docs/plans/expose-admitted-package-navigation-registry-and-plugin-iframe-asset-urls-from-hub.md`: durable approved plan artifact.

## Playbook constraints applied

- Loaded `[[implementer-playbook]]` and `[[botster-implementer-playbook]]`.
- Used `project_pipelines_current_context` and the latest approved plan artifact before editing.
- Ran `cargo update -p botster-core` first and verified both required core contracts before implementing hub code.
- Kept scope to hub-owned public DTO projection, daemon/client routing, generated artifacts, docs, and focused tests.
- Reused core `PackageManifest.navigation` and typed `UiNodeKind::Iframe`; no hub-local replacement shapes were invented.
- Preserved default app-surface discovery for packages without explicit manifest navigation.
- Did not add global plugin ordering authority, priority, sidebar placement, route padding/layout policy, or raw HTML payload support.
- Used `./test.sh` for Rust test evidence and synced generated TypeScript/test-support artifacts.
- On review return, persisted this durable report and prepared the work for PR-policy review with committed branch work.

## Deviations from plan

- The implementation proves typed iframe URL/reference flow through the daemon `PluginSurfaceRender` path and validated UI tree snapshot, with no raw HTML fields in the parent UiNode payload.
- It does not add a new hub static HTTP asset-serving route for `/packages/<package>/assets/<file>`. This is now explicitly documented as a package-scoped URL reference for the client package bridge to resolve. Adding a new asset server would broaden the approved implementation beyond the existing daemon protocol surface.

## Tests run

- `cargo update -p botster-core` passed; `Cargo.lock` updated and the refreshed core contract was verified.
- `cargo fmt` passed.
- `./test.sh -p botster-hub-client` passed: 32 unit tests and 4 doctests.
- `./test.sh --test hub_client_api_test package_navigation` passed: 2 tests.
- `./test.sh --test hub_daemon_lifecycle_test daemon_lists_admitted_package_navigation_with_default_app_surface_fallback` passed: 1 live daemon test.
- `./test.sh --test hub_daemon_lifecycle_test daemon_package_dtos_expose_declared_surfaces_and_validate_surface_ids` passed: 1 live daemon test, including typed iframe URL/reference and no raw HTML field assertions.
- `./test.sh --test hub_plugin_lifecycle_test -- --list` passed and compiled the integration test binary after manifest fixture updates.
- `./test.sh --test hub_plugin_lifecycle_test enabled_provider_package_loads_through_same_core_worker_path` passed: 1 test.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` passed.
- `git diff --check` passed.

## Unverified behavior or residual risk

- Browser/client bridge fetching of `/packages/<package>/assets/<file>` was not implemented or verified in this hub change. The verified behavior is the daemon/runtime path returning a typed iframe UiNode with a package-scoped URL reference and no raw HTML fields.
- Full repository test suite was not run in Implement. Focused protocol, client API, live daemon, plugin lifecycle fixture, generated asset, format, and whitespace checks passed.

## Missing vault guidance discovered

- No missing vault guidance was discovered during implementation.
- Durable protocol guidance was captured in `docs/client-protocol.md`.
- The review suggested a future note on proving asset references have producer/read paths; capture should wait until the product decision is settled on whether the hub daemon or browser package bridge owns static asset reads.
