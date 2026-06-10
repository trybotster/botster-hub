---
description: Plan for printing a real packaged botster-web browser UI URL from hub dogfood.
---

# Print real packaged botster-web UI URL from hub dogfood

## Context loaded

- Project Pipelines context loaded with `project_pipelines_current_context` for ticket `ticket_1781116622_387872`, run `run_1781119586_530735`, run step `run_step_1781119586_257334`, current step `botster_plan`, gate `botster_plan_gate`.
- Pipeline state: dependency `Serve compiled botster-web UI from the package bridge entrypoint` is closed; no prior artifacts, findings, reviews, open questions, or question answers were present.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[dogfood bridge url labels api bridge not browser ui]], [[browser dogfood clients derive bridge url per runtime]], [[plan steps need reviewable plan artifacts]], and [[test script required for rust tests not cargo test]].
- Repo context inspected: `src/main.rs`, `tests/hub_daemon_lifecycle_test.rs`, `src/entrypoint_supervisor.rs` references through existing tests, existing `docs/plans/*`, `Cargo.toml`, and `test.sh`.
- Project Pipelines checklist discipline: attempted `project_pipelines_create_vault_checklist` for run `run_1781119586_530735`; the plugin worker timed out. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence record vault-note provenance, convention conflicts, verification expectations, and durable capture decisions directly.

## Scope

- Update `botster-hub dogfood --web-package-path <botster-web package>` so the existing supervised `botster-web` package entrypoint is treated as both the API bridge and packaged browser UI server when the package serves compiled UI assets.
- Keep `bridge=http://127.0.0.1:<port>` as the diagnostic/API bridge URL.
- Print `web=http://127.0.0.1:<port>/?dogfood=real-hub` only after the same supervised process and selected port return usable browser HTML from the packaged UI server.
- Keep readiness gated on both existing-hub bridge health and packaged UI HTML readiness.
- Preserve dynamic/free bridge port selection, explicit `--web-bridge-port` override behavior, `/tmp` generated data-dir default, explicit `--data-dir` rerun idempotency, existing-hub socket ownership mode, and bounded diagnostics from the prior dogfood fixes.
- Update integration tests so the production `botster-hub dogfood` path proves a no-data-dir run prints a usable web URL, explicit data-dir reruns stay idempotent, package state reports the botster-web entrypoint running, and daemon shutdown cleans the supervised process.

## Non-scope

- No edits to the separate `botster-web` package in this ticket.
- No Vite dependency or Vite dev-server launch path in `botster-hub`.
- No new dogfood ownership mode, package manifest redesign, daemon lifecycle redesign, browser app code, or Project Pipelines UI work.
- No broad cleanup of dogfood startup, session worker smoke checks, package registry behavior, or entrypoint supervision beyond what is needed to verify and print the real packaged UI URL.
- No compatibility fallback that prints `web=` for the raw API bridge when HTML readiness is absent; that would contradict [[dogfood bridge url labels api bridge not browser ui]].

## Assumptions and unknowns

- Assumption: the closed dependency means the `botster-web` package entrypoint now serves both `/health` and the compiled browser app on the same loopback port.
- Assumption: "usable HTML" should be tested by an HTTP GET to `/?dogfood=real-hub` on the selected port, expecting a successful HTML document response rather than JSON not-found or plain `not found`.
- Assumption: the hub does not need to understand Ionic internals; it only needs to prove the package entrypoint serves a browser application shell from the URL it prints.
- Assumption: the correct production path is `dogfood()` -> `start_botster_web_dogfood()` -> `DaemonRequest::StartPackageEntrypoint` -> `/health` readiness and root HTML readiness -> `print_dogfood_ready()`.
- Assumption: the test fixture for `write_botster_web_package` should be updated to model the dependency's new package contract by serving fixture HTML from `/?dogfood=real-hub` while preserving `/health`.
- Unknown: the exact dependency response shape for the real packaged UI root. Implementation should avoid brittle Ionic-specific string checks; prefer status, content type, and a minimal HTML-app-shell marker such as `<!doctype html`, `<html`, or a body containing a script/module reference if the fixture and real package align.
- Unknown: whether `/?dogfood=real-hub` should be the only allowed web URL path. The ticket names that exact printed URL, so plan the readiness probe against that path unless implementation discovers the packaged server normalizes through another equivalent route.
- No human question is needed because the ticket explicitly says to consume the updated package entrypoint contract, not to choose between bridge-only and browser-UI output.

## Botster layers touched

- Rust hub CLI/operator path: `botster-hub dogfood` launch orchestration and ready output.
- Rust hub package entrypoint supervision: existing `StartPackageEntrypoint` and status/list reporting are used; change only if needed for readiness diagnostics.
- Rust hub daemon integration tests: production binary dogfood tests in the daemon lifecycle harness.
- Package fixture tests: synthetic `botster-web` package fixture in tests only, not the real `botster-web` repo.

## Worktree and target assumptions

- Current pipeline run is bound to target id `tgt_7e208a0c76a44980a83b63af976b1f22` and this run worktree.
- Downstream agents should work in their assigned run worktree, not an ambient or base checkout.
- The implementation should not persist raw local worktree paths into repo artifacts or operator output.

## Affected surfaces/files

- `src/main.rs`
  - Extend `DogfoodWebLaunch` with a `web_url` field or equivalent.
  - Add a readiness helper that performs a raw HTTP GET against `/?dogfood=real-hub` on the selected port, validates a 200 HTML response, and rejects JSON not-found responses.
  - Call both existing `/health` readiness and new packaged UI readiness before constructing the final launch result.
  - Change `print_dogfood_ready()` from the current `web=unavailable reason=botster-web-ui-server-not-supervised-by-dogfood` placeholder to `web=<ready packaged UI URL>`.
  - Preserve existing error diagnostics by still checking `failed_web_entrypoint_status()` during waits and keeping returned messages bounded/path-neutral.
- `tests/hub_daemon_lifecycle_test.rs`
  - Update `write_botster_web_package()` so the fixture serves `/health` and an HTML app shell for `/?dogfood=real-hub`; non-UI paths can still return 404.
  - Update `collect_dogfood_ready_output()` to wait for `web=http://127.0.0.1:` instead of `web=unavailable`.
  - Update `cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down()` to assert the printed `web=` URL equals the selected port plus `/?dogfood=real-hub`, fetches HTML, keeps `bridge=` as the same-port diagnostic URL, and still proves `status`, `packages list`, plugin lifecycle state, running entrypoint process, no local path leakage, and shutdown cleanup.
  - Update `cli_dogfood_launcher_uses_generated_data_dir_and_dynamic_bridge_port()` to assert no-data-dir dogfood prints `data_dir=isolated:...`, a dynamic bridge port, and a usable web URL on the same port.
  - Update `cli_dogfood_launcher_reruns_against_existing_explicit_data_dir()` to assert both reruns print ready web URLs on their selected explicit ports and preserve package enabled state.
  - Preserve `cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics()` and add or adapt a fixture if needed so UI readiness failure produces a useful dogfood error.
- `docs/plans/print-real-packaged-botster-web-ui-url-from-hub-dogfood.md`
  - This plan artifact.

## Risks

- A readiness check that only verifies `/health` can still print a `web=` URL whose root returns `not found`. The implementation must probe the exact printed browser URL before printing it.
- A readiness check that keys on too-specific Ionic bundle names can become brittle across compiled asset hashes. Keep the hub check generic: successful HTML document from the packaged server, not framework internals.
- Updating tests only to look for the printed line would miss the user path. At least one integration test must fetch the printed `web=` URL and assert it is HTML, not JSON/error text.
- Changing `bridge=` semantics would regress the diagnostic convention. `bridge=` remains the API bridge label even when the same process also serves UI.
- Supervised process cleanup remains important because dogfood waits on the daemon child. Existing shutdown cleanup assertions should stay in the main dogfood integration test.
- The fixture can accidentally over-model the real package. Keep it minimal: serve a deterministic HTML shell at the printed path and health JSON at `/health`.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`
  - Proves the production binary dogfood path prints `dogfood=ready`, `bridge=http://127.0.0.1:<port>`, and `web=http://127.0.0.1:<port>/?dogfood=real-hub`.
  - Fetches the printed `web=` URL and asserts usable HTML, not `{ "error": "not_found" }` or plain `not found`.
  - Proves `status`, `packages list`, lifecycle state, package entrypoint running state, no local path leakage, and shutdown process cleanup.
- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_uses_generated_data_dir_and_dynamic_bridge_port`
  - Proves no-data-dir dogfood uses `/tmp` isolated data dir, dynamic nonzero bridge port, bridge health, and usable same-port web URL.
- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_reruns_against_existing_explicit_data_dir`
  - Proves explicit data-dir reruns remain idempotent and keep printing ready web URLs on the requested ports after package re-enable.
- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics`
  - Keeps the pre-readiness failure diagnostics path covered.
- `./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_cleans_up_on_disable_remove_and_shutdown`
  - Run if entrypoint supervisor cleanup behavior is touched.
- `./test.sh --test hub_daemon_lifecycle_test`
  - Run for final verification because this ticket touches the dogfood daemon lifecycle harness.
- `cargo fmt`
  - Formatting check after Rust edits.

## Runtime path proof required

Implementation evidence must show the real user path changed:

- `botster-hub dogfood --web-package-path <path>` starts the `botster-web` `web-client` runnable entrypoint through `DaemonRequest::StartPackageEntrypoint`.
- The selected bridge port is passed through `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT` and is the same port used for both `bridge=` and `web=`.
- The `web=` line is printed only after `/?dogfood=real-hub` returns a successful HTML app shell.
- A fetched printed `web=` URL serves browser HTML rather than the old API bridge not-found response.

Evidence that helper code exists is not enough; at least one test or verification note must exercise the compiled `botster-hub dogfood` command path.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/print-real-packaged-botster-web-ui-url-from-hub-dogfood.md`.
- Plan gate evidence should include the loaded context, scope and non-scope, assumptions and unknowns, affected files, risks, acceptance checks, and vault gaps from this document.
- Checklist attempt: `project_pipelines_create_vault_checklist` timed out in the plugin worker. This plan records fallback evidence directly.
- Downstream implementation should submit evidence for runtime behavior, test commands, exact skipped-test reasons if any, and whether durable vault knowledge was captured.

## Vault gaps worth capturing

- Capture if implementation settles a durable hub-side convention for "usable packaged browser UI readiness" checks: generic HTML shell proof, exact printed path, and how to avoid framework-specific asset assertions.
- Capture if the real packaged botster-web contract establishes that the same supervised package entrypoint owns both API bridge and static UI serving in existing-hub mode.
- Capture if dogfood output now has a stable convention: `bridge=` for the API diagnostic endpoint and `web=` only for a verified browser UI URL.
- No new durable knowledge was captured at plan time because the implementation result will determine whether these become standing conventions rather than ticket-local facts.
