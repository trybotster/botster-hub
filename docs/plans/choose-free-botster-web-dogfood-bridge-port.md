---
description: Plan for choosing a free botster-web dogfood bridge port by default.
---

# Choose a free botster-web dogfood bridge port by default

## Context loaded

- Pipeline context loaded for ticket `ticket_1781114465_496008`, run `run_1781114470_619147`, step `botster_plan`, gate `botster_plan_gate`. There are no prior artifacts, findings, reviews, open questions, or answers.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster web dogfood bridge ownership modes are explicit]], [[connection diagnostics derive from distinguishable runtime signals]], [[test script required for rust tests not cargo test]], [[subprocess harnesses must kill child on failed readiness]], [[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]], and [[plan steps need reviewable plan artifacts]].
- Repo context inspected: `src/main.rs`, `src/entrypoint_supervisor.rs`, `tests/hub_local_dogfood_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, existing `docs/plans/*`, `Cargo.toml`, and `test.sh`.
- Workflow checklist evidence fallback: `project_pipelines_create_checklist` timed out in the plugin worker. Per the checklist instructions and [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence record vault notes read, convention conflicts, verification evidence, and capture decision directly.

## Scope

- Change `botster-hub dogfood` so omitting `--web-bridge-port` chooses an available loopback port at runtime instead of defaulting to fixed port `41739`.
- Preserve `--web-bridge-port <port>` as an explicit override and continue passing that exact value to botster-web via `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT`.
- Continue printing the selected URL as `web=http://127.0.0.1:<port>` from the real dogfood ready output.
- Improve pre-readiness botster-web failure diagnostics so an entrypoint process that exits or fails before `/health` becomes ready reports bounded supervised entrypoint diagnostics/stderr instead of only the last health connection error.
- Add focused tests for dynamic default port selection, explicit override preservation, and diagnostic surfacing without PII.

## Non-scope

- No changes to `botster-web`, `botster-core`, or `botster-tui`.
- No new dogfood ownership mode, daemon lifecycle redesign, package manifest redesign, or browser UI work.
- No broad cleanup of dogfood startup, session worker smoke checks, package registry behavior, or health response shape beyond what is necessary to satisfy this ticket.
- No optional user-facing configuration beyond the existing `--web-bridge-port` override.

## Assumptions and unknowns

- Assumption: "choose a free port" means bind an ephemeral `127.0.0.1:0` listener, read `local_addr().port()`, drop the listener before spawning botster-web, and pass that selected port. This is the smallest standard-library change; it cannot eliminate the normal bind race between selection and child process startup, but it avoids dependence on one fixed port.
- Assumption: the default dynamic port should be selected during dogfood option resolution or immediately before `start_botster_web_dogfood`, while explicit overrides should skip probing and use the requested value exactly.
- Assumption: diagnostics should be derived from distinguishable runtime signals: health polling connection failures remain health failures while entrypoint exited/failed status plus diagnostics become the higher-signal startup failure.
- Unknown: whether current daemon entrypoint status refresh drains stderr quickly enough while health polling is running. If not, implementation should poll `PackageEntrypointStatus` during the readiness loop and use the latest process diagnostics snapshot.
- Unknown: whether existing tests can call private `src/main.rs` helpers directly. If not, put the minimal test seam in the same file's `#[cfg(test)]` module or cover through daemon lifecycle integration helpers without exposing a public API.
- No human question is needed because the ticket explicitly defines the intended default behavior, override behavior, diagnostics, and excluded repositories.

## Botster layers touched

- Rust hub CLI/operator path: `botster-hub dogfood` option parsing, launch orchestration, ready output.
- Rust hub package entrypoint supervision/status: only as needed to surface existing bounded stdout/stderr diagnostics while waiting for botster-web health.
- Rust hub tests: focused unit/integration coverage through repo-standard `./test.sh`.

## Affected surfaces/files

- `src/main.rs`
  - Replace `DogfoodOptions.web_bridge_port: u16` with a shape that distinguishes explicit override from dynamic default, for example an enum or `Option<u16>` resolved after parsing.
  - Add a small helper that selects a loopback ephemeral port with `TcpListener::bind(("127.0.0.1", 0))`, returns the assigned port, and maps bind/local-address errors into `DogfoodError`.
  - Ensure `start_botster_web_dogfood` receives the final selected port and still writes `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT` plus `web=http://127.0.0.1:<port>`.
  - Change botster-web health waiting to accept enough context to query the supervised `botster-web` `web-client` entrypoint status while waiting. If the entrypoint exits/fails, return a `DogfoodError::WebEntrypointStart` or similar message that includes process state, exit status, and bounded diagnostics from the supervisor.
  - Keep diagnostic messages path-neutral and bounded; rely on existing supervisor redaction where possible and avoid printing raw environment values.
- `src/entrypoint_supervisor.rs`
  - Likely unchanged because it already captures bounded stdout/stderr and redacts `$HOME`/current directory. Touch only if the diagnostics snapshot is not refreshed or bounded enough for this user path.
- `tests/hub_local_dogfood_test.rs`
  - Add focused helper coverage if practical for dynamic default selection and explicit override resolution.
  - Add a failure fixture or test package entrypoint that writes a recognizable non-PII stderr line and exits before health readiness, then assert dogfood startup reports that diagnostic instead of only `connect botster-web health`.
- `tests/hub_daemon_lifecycle_test.rs`
  - Use existing package entrypoint supervision tests if a daemon-level test is the cleanest way to prove failed entrypoint diagnostics are exposed.
- `docs/plans/choose-free-botster-web-dogfood-bridge-port.md`
  - This plan artifact.

## Risks

- Ephemeral port selection has a small time-of-check/time-of-use race after dropping the listener. Holding the listener until child startup would require botster-web support or socket passing, which is outside the ticket and would touch botster-web.
- A health wait loop that only records connection-refused errors will keep hiding the real entrypoint failure. The implementation must poll entrypoint status after launch, not just improve the final health error text.
- Diagnostic surfacing can leak local paths if it bypasses existing supervisor redaction. Tests should assert no user home path or current worktree path appears in the returned error.
- Over-broad diagnostics can confuse connection states. Follow [[connection diagnostics derive from distinguishable runtime signals]]: report entrypoint process failure only when the supervised process state proves it.
- Tests that only exercise helper functions could miss the production path. At least one test or verification note must identify the actual `dogfood -> start_botster_web_dogfood -> StartPackageEntrypoint -> wait health/status -> print_dogfood_ready` path.

## Acceptance checks/tests

- Focused parser/default test:
  - Proves `DogfoodOptions::parse(["--web-package-path", "..."])` no longer resolves to fixed `41739` and the runtime default path selects a nonzero loopback port.
  - Proves `DogfoodOptions::parse(["--web-package-path", "...", "--web-bridge-port", "41740"])` preserves `41740`.
- Focused runtime launch test:
  - Starts the dogfood botster-web launch path or the narrowest daemon-backed equivalent.
  - Asserts `StartPackageEntrypoint.environment_overrides["BOTSTER_WEB_DOGFOOD_BRIDGE_PORT"]` receives the selected default port for omitted override and `41740` for explicit override.
  - Asserts `DogfoodWebLaunch.bridge_url` and/or ready output includes `web=http://127.0.0.1:<selected-port>`.
- Failure diagnostics test:
  - Uses a test botster-web-style package entrypoint that writes a bounded marker such as `bridge bind failed: fixture` to stderr and exits before health readiness.
  - Asserts dogfood reports the supervised entrypoint diagnostic and does not report only the final `connect botster-web health` error.
  - Asserts the error text does not contain local home/worktree paths or raw environment values.
- Repo commands:
  - `./test.sh --test hub_local_dogfood_test <new_dynamic_port_test_name>`
  - `./test.sh --test hub_local_dogfood_test <new_explicit_override_test_name>`
  - `./test.sh --test hub_local_dogfood_test <new_entrypoint_diagnostics_test_name>` or the corresponding focused `hub_daemon_lifecycle_test` names if the test belongs there.
  - `./test.sh --test hub_local_dogfood_test`
  - `./test.sh --test hub_daemon_lifecycle_test` if entrypoint supervisor or daemon request behavior is touched.
  - `cargo fmt`

## Runtime path proof required

Implementation evidence must show the real one-command dogfood path changed:

- `dogfood()` parses options and resolves a dynamic bridge port when `--web-bridge-port` is absent.
- `start_botster_web_dogfood()` passes the selected port into `DaemonRequest::StartPackageEntrypoint.environment_overrides` as `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT`.
- `print_dogfood_ready()` prints the same selected port in `web=http://127.0.0.1:<port>`.
- The health/readiness failure path queries supervised entrypoint status or otherwise consumes the production supervisor diagnostics before returning the final error.

Evidence that a helper exists is not enough; tests or verification notes must identify the compiled path from `botster-hub dogfood` to the supervised botster-web entrypoint.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/choose-free-botster-web-dogfood-bridge-port.md`.
- Gate evidence should include the loaded context, scope/non-scope, assumptions, affected files, risks, acceptance checks, vault gaps, and the checklist timeout fallback.
- Downstream implementation should report dynamic default behavior, explicit override behavior, diagnostic surfacing, command evidence, and any skipped tests with exact reasons.

## Vault gaps worth capturing

- Capture if the final implementation settles a reusable Botster convention for ephemeral loopback port selection in dogfood/accessory launchers.
- Capture if supervised entrypoint diagnostics need a standard "startup failed before readiness" helper across dogfood and package entrypoint commands.
- Capture if the existing supervisor redaction is insufficient for non-PII diagnostics and needs a broader path/environment scrubbing convention.
- No new durable knowledge was captured at plan time; existing notes cover dogfood ownership modes, distinguishable diagnostics, test command discipline, process cleanup, and plan artifact hygiene.
