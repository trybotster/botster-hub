# Add Botster Hub Doctor And Smoke Commands For Local Runtime

## Context Loaded

- Ticket: `ticket_1783032083_582448`, "Add botster-hub doctor and smoke commands for local runtime".
- Run/step: `run_1783047971_614814`, `botster_plan`, with no open findings, questions, prior answers, or artifacts in the current context.
- Dependency context: the prerequisite "Add botster-hub up/down local runtime commands" is closed. Current repo already has `botster-hub up`, `down`, `dev-stack bootstrap`, `dogfood`, `status`, `apps`, `packages`, `sessions`, `run-one`, daemon compatibility descriptors, local WebRTC signaling, and package entrypoint supervision.
- Vault/playbook context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Skill context: `botster-customize-hub` applies because this adds top-level hub commands. The relevant rule is to keep hub/CLI orchestration in the hub runtime path, use real client transport or internal client APIs, and avoid side-channel command events.
- Repo evidence inspected: `src/main.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_dogfood_test.rs`, `tests/hub_client_api_test.rs`, `README.md`, and existing `docs/plans/*` around daemon status, dogfood, dev stack, app registry, WebRTC, compatibility, and local runtime.
- Workflow note: Project Pipelines checklist creation timed out in the plugin worker. Preserve checklist evidence in the gate submission per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Add top-level `botster-hub doctor` as a non-mutating local-runtime diagnostic command over `--data-dir`.
- Add top-level `botster-hub smoke` as the user-facing conclusive local-runtime proof command over `--data-dir`.
- Reuse existing daemon socket protocol, `DaemonCompatibility`, `DaemonStatus`, package/app DTOs, package entrypoint supervision, local WebRTC signaling, and session attach/drain paths.
- Emit structured, human-readable check rows with stable names and severities such as `pass`, `warn`, and `fail`, plus concise remediation lines.
- Make stale/incompatible daemon reporting explicit for doctor, including pre-compatibility hello/status responses that currently become `DaemonTransportError::Compatibility`.
- Make smoke either prove daemon/core/package/app/session/WebRTC terminal path using first-party local packages, or fail with a precise missing-prerequisite diagnostic.
- Add focused CLI tests for doctor healthy, stopped, and stale/incompatible daemon cases.
- Add focused CLI smoke coverage that proves the production `botster-hub smoke` command uses the real daemon/user path, not only in-process helper code.
- Update README local runtime docs to include doctor/smoke as daily commands and describe missing-prerequisite outcomes.

## Non-Scope

- No removal or renaming of lower-level commands (`status`, `sessions`, `apps`, `packages`, `dogfood`, `dev-stack`, `run-one`, `up`, `down`).
- No new browser harness ownership if existing package/hub mechanics can drive botster-web. Invoke or reuse the package path rather than duplicating botster-web internals.
- No cloud/WebRTC relay, remote pairing, OAuth, marketplace fetch, hosted registry, or browser UI changes.
- No generic health-check framework, plugin policy engine, or broad daemon refactor.
- No new dependency unless implementation proves the standard library and existing crates cannot perform the required checks.
- No PII or raw package source path leakage in doctor/smoke output. `--data-dir` may appear only where existing local-runtime output already prints the operator-selected directory for copyable remediation.

## Botster Layers Touched

- Rust hub CLI: command dispatch, option parsing, structured output, user-facing errors.
- Rust daemon/client protocol: read existing status/compatibility/package/app/session/WebRTC DTOs; add narrow DTO fields only if a required check cannot be expressed from current public surfaces.
- Package/app runtime: inspect installed packages, enabled states, runnable entrypoints, app rows, `local_url`, and supervisor state.
- Local WebRTC/session data plane: smoke should drive signaling/DataChannel/terminal attach through the existing local WebRTC adapter and shared terminal subscription path when botster-web/local packages are available.
- Docs/tests: README plus Rust integration tests through the compiled `botster-hub` binary.

## Assumptions And Unknowns

- Assumption: `doctor` is diagnostic and should not start, stop, install, enable, reload, or rebuild packages. It can report missing prerequisites and remediation commands.
- Assumption: `smoke` may be mutating because it is a proof command. It can start/reuse the daemon, enable first-party local packages, start package entrypoints, spawn a disposable session, exercise the transport path, and then clean up its own session/processes where it owns them.
- Assumption: `smoke` should share as much code as practical with `up`/`dev-stack bootstrap` and existing WebRTC tests, but its CLI output should be purpose-built: `smoke=pass` or `smoke=fail` plus named proof rows.
- Assumption: first-party package discovery can follow the current `DevStackOptions` defaults and flags. Missing sibling packages should produce `missing_prerequisite` diagnostics, not fallback to fake success.
- Unknown: whether the common-case `smoke` should default to the same stable data dir as `up` or require explicit `--data-dir`. The ticket acceptance names `--data-dir <dir>`, so prefer requiring/parsing it first unless current `up` default behavior is intentionally shared.
- Unknown: whether a full browser-driven botster-web smoke is available in this repo without duplicating downstream harness code. Prefer hub-side local WebRTC/DataChannel proof through existing Rust test machinery and package launch DTOs; only invoke external browser harnesses if they already have a documented command.
- Unknown: exact output vocabulary. Prefer stable key/value lines matching existing CLI style (`check name=... status=...`) over JSON unless implementation finds an existing structured output convention to reuse.

## Affected Surfaces And Files

- `src/main.rs`
  - Add `doctor` and `smoke` dispatch arms.
  - Add options structs for `doctor` and `smoke`, likely reusing `DataDirOptions` and `DevStackOptions` parsing patterns.
  - Add small diagnostic/check row rendering helpers.
  - Add doctor implementation using `daemon_transport_request(Status)`, `ListPackages`, `ListApps`, and package entrypoint status where reachable.
  - Add smoke implementation by composing `prepare_local_runtime`, app/package checks, session spawn/attach/input/drain cleanup, and local WebRTC proof helpers where available.
  - Extend usage text.
- `crates/botster-hub-client/src/lib.rs`
  - Touch only if doctor/smoke need a typed diagnostic enum/field currently unavailable from public client DTOs. Existing `DaemonDiagnostic`, compatibility errors, status, package, app, and WebRTC request DTOs should be preferred.
- `src/daemon_transport.rs`
  - Touch only if a production diagnostic cannot be observed over the current status/package/app/WebRTC request path.
- `src/local_webrtc.rs`
  - Touch only if smoke needs a small reusable hub-side proof helper; do not duplicate WebRTC transport ownership.
- `crates/botster-hub-test-support/src/lib.rs`
  - Prefer adding reusable test-support proof helpers here if external-client-shaped tests need to share hub/session-worker subprocess setup.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add CLI tests for doctor healthy, stopped, stale/incompatible.
  - Add CLI smoke test for success with local first-party fixtures and missing-prerequisite failure.
- `tests/hub_client_api_test.rs` or `tests/hub_local_dogfood_test.rs`
  - Touch only for lower-level regression coverage if new helper behavior is introduced below the CLI layer.
- `README.md`
  - Document `doctor` and `smoke` in the local runtime section.
- `docs/plans/add-botster-hub-doctor-and-smoke-commands-for-local-runtime.md`
  - This plan artifact.

## Implementation Plan

1. Add a tiny `RuntimeCheck`/`RuntimeCheckStatus` CLI-local shape in `src/main.rs` unless an existing public diagnostic row cleanly fits all output. Keep it private to CLI formatting if it is only presentation.
2. Implement `doctor --data-dir <dir>`:
   - Build explicit config.
   - Check daemon socket status via real handshake/request.
   - Map `NotRunning` to a stopped diagnostic with `botster-hub up --data-dir <dir>`.
   - Map `Compatibility`/protocol errors to stale/incompatible diagnostics with the same remediation language already used by `up/down`.
   - On healthy status, print daemon lifecycle, protocol version, conformance fixture revision, core initialized, package/provider counts, recovered/stale sessions.
   - Query packages/apps when status succeeds and print package registry, enabled package count, botster-web app entrypoint state/local URL if present, and missing first-party package/app warnings where relevant.
3. Implement `smoke --data-dir <dir>`:
   - Reuse `prepare_local_runtime` or a narrow shared helper so daemon startup, package enablement, botster-web launch, and readiness checks match `up`.
   - Prove daemon/core via `Status`.
   - Prove package/app via `ListPackages`/`ListApps` and botster-web `web-client` structured `local_url`.
   - Prove session/PTI path by spawning a disposable session, attaching, sending input, draining for a marker, and shutting the session down through the daemon socket.
   - Prove local WebRTC terminal path by using the existing local WebRTC signal request/data-channel route where available in this crate's test/runtime helpers. If first-party web package or WebRTC prerequisites are absent, fail with a named missing-prerequisite diagnostic instead of silently downgrading.
4. Keep cleanup explicit: shut down disposable smoke sessions, detach subscriptions, and avoid stopping a pre-existing daemon unless the smoke command clearly started and owns it.
5. Update usage and README.

## Risks

- Underwired command risk: adding output without driving the daemon socket/package/app/session/WebRTC paths would not satisfy the ticket. Tests must execute the compiled CLI commands.
- Compatibility masking risk: doctor must distinguish stopped, incompatible/stale, and ordinary malformed transport errors without converting every JSON/protocol failure into stale-daemon advice.
- Cleanup risk: smoke can leave sessions, package entrypoints, or daemon children behind. Use existing daemon shutdown/session shutdown helpers and test cleanup.
- Scope creep risk: doctor can turn into a generic framework. Keep checks directly tied to ticket-listed runtime surfaces.
- Path/PII risk: package source paths and local home paths can leak through error formatting. Assert absence of home-directory prefixes such as `<home>/` and fixture package paths in healthy output where practical.
- WebRTC proof risk: duplicating botster-web browser harness code would create ownership drift. Prefer existing local WebRTC adapter/test-support paths or invoke documented external harnesses explicitly.
- Test runtime risk: live daemon/WebRTC tests are serialized and can be slow/flaky. Keep focused tests small and use existing daemon test lock/readiness helpers.

## Acceptance Checks And Tests

- `./test.sh --test hub_daemon_lifecycle_test cli_doctor_reports_stopped_runtime_with_remediation -- --test-threads=1`
  - `botster-hub doctor --data-dir <dir>` exits nonzero or warning-class per chosen contract, reports daemon stopped/not running, and suggests `botster-hub up --data-dir <dir>`.
- `./test.sh --test hub_daemon_lifecycle_test cli_doctor_reports_healthy_runtime_checks -- --test-threads=1`
  - Starts a real daemon, runs doctor, asserts pass rows for daemon running, protocol/conformance revision, core initialized, package registry, and no raw local path leakage.
- `./test.sh --test hub_daemon_lifecycle_test cli_doctor_reports_incompatible_stale_daemon_without_deleting_socket -- --test-threads=1`
  - Uses a fake pre-compatibility socket like the existing `up` regression and proves doctor reports stale/incompatible with actionable remediation while preserving the live socket file.
- `./test.sh --test hub_daemon_lifecycle_test cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc -- --test-threads=1`
  - Runs `botster-hub smoke --data-dir <dir>` with first-party fixtures and asserts proof rows for daemon/core/packages/apps/session/WebRTC terminal path.
- `./test.sh --test hub_daemon_lifecycle_test cli_smoke_reports_missing_first_party_prerequisites -- --test-threads=1`
  - Runs smoke without required first-party package fixture(s) and asserts precise missing-prerequisite diagnostics.
- Existing focused regressions:
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_reports_incompatible_daemon_without_deleting_socket -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_acceptance_smoke_exercises_first_party_plugins_project_pipelines_session_templates_reload_and_shutdown -- --test-threads=1`
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_client_reports_compatibility_descriptor_and_mismatch_diagnostics -- --test-threads=1`
- If client DTOs or generated protocol change:
  - `./test.sh --test hub_client_api_test`
  - TypeScript protocol generation/drift test already present in the client crate, if touched.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this `docs/plans/` artifact plus loaded vault notes.
- Implement gate must prove the production user path changed by running compiled CLI `doctor`/`smoke` commands, not only unit helpers.
- Review should reject unwired structs, fake WebRTC success, missing stale-daemon coverage, missing cleanup, raw path leakage, or hidden browser harness duplication.

## Convention Conflicts

None found. The plan follows the loaded Botster conventions: CLI remains thin, hub owns local runtime policy, core/session/client actors own terminal data-plane mechanics, local clients use public daemon/client DTOs, package/app status comes from daemon projections, and Project Pipelines artifacts use note titles/wiki links rather than raw vault paths.

## Vault Gaps Worth Capturing

- Capture the final `doctor` output vocabulary and stale-daemon remediation contract if implementation settles a reusable pattern for health checks.
- Capture the final `smoke` proof boundary: whether WebRTC is proven through a Rust local DataChannel peer, an invoked first-party browser harness, or a documented missing-prerequisite diagnostic.
- Capture any new cleanup convention if smoke introduces ownership rules for started/reused daemons or package entrypoints beyond existing `up`/`dev-stack` behavior.
