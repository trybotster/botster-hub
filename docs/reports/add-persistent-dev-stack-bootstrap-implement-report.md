# Add Persistent Dev-Stack Bootstrap Implement Report

## Summary

Implemented `botster-hub dev-stack bootstrap` as a persistent first-party local bootstrap path. The command starts or reuses a real daemon for a stable data directory, enables `project-pipelines`, `botster-web`, `botster-tui`, and `botster-workspaces` through daemon package requests, starts the `botster-web` package app entrypoint, and prints actionable follow-up commands from the same data dir.

## Assumptions

- The Project Pipelines package source remains the checked-in `examples/project-pipelines` manifest named `project-pipelines`.
- `botster-web`, `botster-tui`, and `botster-workspaces` can be discovered from sibling checkouts for human use, but tests must pass explicit fixture paths.
- `botster-workspaces` may be plugin-only today; bootstrap enables it and only starts app entrypoints advertised through the app registry.
- Printing the selected data dir is acceptable because it is required for copy/paste operator commands; package source paths remain omitted from normal command output.

## Files Changed

- `src/main.rs`
  - Added `dev-stack bootstrap` CLI parsing and usage.
  - Added stable default data dir `target/botster-hub-dev-stack-data`.
  - Added start-or-reuse daemon bootstrap using `botster-hub start --data-dir`.
  - Added first-party package path flags and daemon-owned enablement.
  - Kept app startup on `StartPackageEntrypoint` and app URL confirmation on `ListApps`.
- `tests/hub_daemon_lifecycle_test.rs`
  - Added explicit fixture-path dev-stack bootstrap tests.
  - Covered live daemon reuse, post-shutdown persisted state reload, no duplicate enabled package rows, app registry output, and package path redaction.
- `README.md`
  - Documented `dev-stack bootstrap` as the daily persistent first-party local path.
  - Left `dogfood` as compatibility/test launcher documentation.
- `docs/reports/add-persistent-dev-stack-bootstrap-implement-report.md`
  - Durable implementation handoff report.

## Constraints Applied

- Loaded `[[implementer-playbook]]` and `[[botster-implementer-playbook]]`.
- Applied Botster package/app constraints from the approved plan: daemon-owned package mutation, persisted hub-state registry, installed app rows from package runnable entrypoints, exact app selectors, daemon-resolved terminal launch contracts, and local runnable packages still needing core entrypoints.
- Kept first-party packages as ordinary local package manifests, not built-ins or embedded hub code.
- Kept tests fixture-driven with explicit package paths; sibling checkout discovery is operator convenience only.
- Used `./test.sh` for Rust tests.

## Deviations From Plan

- No new daemon DTOs or transport changes were needed.
- The implementation reuses existing botster-web dogfood bridge verification helpers while adding app URL confirmation through `ListApps`.
- No PR was created in this step; gate evidence records the local diff and durable report artifact.

## Verification

- `cargo fmt`
- `git diff --check`
- `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_bootstrap -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test daemon_list_apps_projects_installed_package_entrypoints -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test daemon_resolves_terminal_app_foreground_launch_contract -- --test-threads=1`
- `./test.sh --test hub_daemon_lifecycle_test cli_packages_enable_botster_workspaces_first_party_plugin_db_namespace -- --test-threads=1`

One attempted combined regression invocation failed because the repo wrapper forwards to `cargo test`, which accepts only one test filter. The same targets were rerun individually and passed.

## Residual Risk

- Manual sibling checkout discovery was not smoke-tested against real sibling repos in this worktree.
- The actual first-party `botster-workspaces` sibling package was not available in the fixture path; coverage uses the existing local package fixture shape with the expected package name and plugin DB scope.
- The command currently reports `daemon=started` based on spawning and readiness, not on an OS-level parent ownership handle after the bootstrap process exits.

## Missing Vault Guidance

No blocking vault guidance was missing. A future durable note may be worth capturing if `dev-stack bootstrap` becomes the stable naming convention for persistent first-party local package bootstrap.
