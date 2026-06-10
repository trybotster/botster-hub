---
title: Diagnose and fix real-hub dogfood Spawn runtime failure
ticket: ticket_1781123821_215380
run: run_1781124194_318566
step: botster_plan
---

# Diagnose and fix real-hub dogfood Spawn runtime failure

## Context loaded

- Pipeline context loaded with `project_pipelines_current_context`: run `run_1781124194_318566`, step `botster_plan`, ticket `ticket_1781123821_215380`. There are no prior artifacts, findings, reviews, questions, or answers for this run.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster overlay context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[connection diagnostics derive from distinguishable runtime signals]], [[botster web dogfood bridge ownership modes are explicit]], [[coredaemon embedding without worker path creates in process sessions]], [[botster session worker requires explicit build in dogfood launchers]], [[daemon request errors should return operator frames without dropping transport]], [[botster hub diagnostics use daemon diagnostic rows in client dtos]], [[botster web dto field names must match authoritative rust serde structs]], and [[test script required for rust tests not cargo test]].
- Project Pipelines checklist discipline: `project_pipelines_checklist_instructions` was loaded. The initial `project_pipelines_create_vault_checklist` response timed out with `plugin worker invoke timeout`, but the checklist was later visible as `checklist_1781124252_897541`; update the checklist items as this revised Plan closes Plan Review findings. If checklist updates fail later, per [[project pipelines checklist worker timeouts require artifact evidence fallback]], preserve evidence in this plan and the gate payload.
- Repo context inspected:
  - `docs/client-protocol.md` documents the production route: `botster_hub_client::DaemonConnection::request -> src/daemon_transport.rs serve_daemon/handle_connection -> handle_runtime_control_request -> HubClientApi::handle_request -> HubRuntime -> CoreDaemon`.
  - `src/main.rs` `dogfood` starts a real daemon with `--session-worker-bin`, verifies `dogfood-worker-smoke` via `DaemonRequest::Spawn`, enables `project-pipelines`, then starts the `botster-web` package entrypoint with `BOTSTER_HUB_SOCKET`, `BOTSTER_HUB_DATA_DIR`, and `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT`.
  - `src/client_api.rs` currently collapses most `CoreDaemonError` values into `HubClientRuntimeErrorKind::Runtime`, losing the underlying spawn failure detail.
  - `src/daemon_transport.rs` currently renders that collapsed value as `runtime failed while handling Spawn: Runtime`; diagnostics are only added for unknown-session attach/drain, not Spawn.
  - `src/main.rs` `verify_dogfood_session_worker` already performs a successful `DaemonRequest::Spawn` for `dogfood-worker-smoke` during dogfood startup with an explicit `--session-worker-bin`. That de-prioritizes missing or wrong worker path as the first hypothesis for the later web action failure.
  - `tests/hub_daemon_lifecycle_test.rs` already covers launcher startup, package panels, failed web entrypoint diagnostics, request-level operator errors, and external client attach/drain, but not the packaged web Spawn action request shape named in this ticket.
- Plan Review context loaded on revision: `review_1781124659_154782` returned changes required with one blocker, two medium findings, and one low finding. This revision addresses all four: mandatory web-user-path proof, missing diagnostics DTO notes, sharper root-cause differential, and a human-question gate for session-id reuse.

## Scope and non-scope

Scope:

- Reproduce the failing packaged botster-web real-hub spawn request shape against a real local hub daemon and session-worker path, not only `HubClientApi` in-process fixtures.
- Diagnose whether the failure is hub-owned by tracing `DaemonRequest::Spawn` through `daemon_transport`, `HubClientApi`, `HubRuntime`, `CoreDaemon`, and the configured `botster-session-worker`.
- If hub-owned, fix the smallest failing surface: likely request construction, session-id handling, worker-path materialization/configuration, or daemon/operator error mapping.
- Improve Spawn operator error details so the web client receives a bounded actionable diagnostic instead of only `runtime failed while handling Spawn: Runtime`.
- Prove the web user path consumes the improved diagnostic: identify the botster-web production entry point that maps public daemon `DaemonDiagnostic` rows into the UI/action-failed state, and require verification that the improved `DaemonResponse` fields render or are generically surfaced there.
- Add or strengthen a daemon-socket integration smoke proving the botster-web request shape can spawn `botster-web-dogfood-session`, list it, attach/drain output, and surface a useful diagnostic on intentional failure.
- Ensure successful spawn makes the daemon-visible session list and package/entrypoint process rows observable so session/package panels can update.

Non-scope:

- No broad refactor of `HubRuntime`, `CoreDaemon`, session-worker internals, package registry, entrypoint supervisor, or the client protocol.
- No new abstraction layer for error handling; extend the existing `HubClientError`, `DaemonOperatorError`, and `DaemonDiagnostic` mapping only as needed.
- No changes to botster-web unless hub evidence proves the hub path is already correct and the failure is outside this repo.
- No hidden fallback that masks a core runtime bug. If the exact real request fails inside botster-core after hub mapping is proven correct, stop and create/mark a core dependency ticket with the failing request, command, session id, worker path evidence, and daemon response.
- No PII or raw local path leakage in operator/browser-facing success or error text.

## Assumptions and unknowns

- Assumption: "packaged botster-web spawn action" means the same same-device daemon protocol shape the browser bridge uses: `DaemonRequest::Spawn { session_id: "botster-web-dogfood-session", command: ... }`, followed by `ListSessions`, `Attach`, `Drain`, and later `ShutdownSession`.
- Assumption: The implementation can add a hub-side fixture that mimics the botster-web bridge request shape without requiring the real botster-web checkout in CI. If the real checkout is locally available, use it as an additional manual smoke, not the only proof.
- Assumption: The existing dogfood launcher should remain in existing-hub attach mode for botster-web; it should not spawn or clean up a second hub from the web bridge.
- Unknown: The exact command string the real botster-web package sends from the UI action. The implementer should confirm from the local botster-web package or captured request logs before finalizing the regression test. If there are multiple plausible command shapes, ask a human question rather than picking silently.
- Unknown: Whether the root failure is command shell construction, session-id reuse, web-bridge transport mapping, or a core session-worker spawn error. Because `dogfood-worker-smoke` already succeeds through the same daemon with `--session-worker-bin`, treat missing/incorrect worker path as a lower-probability fallback hypothesis unless the captured raw error points there.
- Unknown: Whether `HubClientRuntimeErrorKind` should grow a new stable `SpawnFailed`/`WorkerUnavailable` category or keep `Runtime` with richer diagnostics. Prefer the smallest additive stable protocol change that gives the browser actionable detail.
- Unknown: If instrumentation proves `botster-web-dogfood-session` reuse is the root cause, the product contract is ambiguous. The implementer must ask a human whether repeated action should reject with a duplicate-session diagnostic or perform explicit cleanup-then-respawn; do not choose silently.

## Affected surfaces/files

- `src/client_api.rs`: likely change to preserve enough classified runtime failure detail from `CoreDaemonError` during `HubClientRequest::Spawn`.
- `src/daemon_transport.rs`: likely change to map Spawn runtime failures into structured `DaemonOperatorError` diagnostics and keep the daemon responsive.
- `crates/botster-hub-client/src/lib.rs`: possible additive `DaemonDiagnosticKind` or DTO fields if existing `action_failure`/`terminal_stream_unavailable` cannot accurately describe Spawn failures.
- `src/main.rs`: possible narrow CLI/dogfood smoke adjustment if `verify_dogfood_session_worker` or operator rendering hides the evidence needed by real web dogfood.
- `tests/hub_daemon_lifecycle_test.rs`: add/extend serialized real-daemon tests for the botster-web spawn shape and intentional failure diagnostics.
- `tests/support/mod.rs`: likely reuse `ensure_session_worker_binary`; avoid new worker discovery code unless needed by the test harness.
- botster-web checkout/package, if available locally: inspect the production bridge/action code that consumes `DaemonResponse.error` and `DaemonResponse.diagnostics`. This is a required user-path proof input, not optional manual polish.
- `docs/client-protocol.md` and `README.md`: update only if public diagnostics or the documented dogfood smoke contract changes.

## Proposed implementation plan

1. Reproduce and instrument the real path.
   - Start from the existing real daemon test harness (`start_cli_daemon`, `botster_hub_client::DaemonConnection`, `ensure_session_worker_binary`).
   - Send the web-shaped Spawn request for `botster-web-dogfood-session` over the daemon socket.
   - Record whether failure happens before daemon request handling, in `HubClientApi::Spawn`, in `HubRuntime::spawn_session`, in `CoreDaemon::spawn`, or in worker process startup.
   - Weight first-pass instrumentation toward web request shape: command string, session id, and bridge transport mapping. Keep worker-path checks in the evidence packet, but do not treat worker path as co-equal unless the raw error contradicts the successful startup spawn smoke.

2. Fix the hub-owned root cause, if present.
   - If the command string/session id/request mapping is wrong, fix the mapper or launcher/web-bridge contract at the hub boundary.
   - If the worker path is missing or wrong in hub startup, fix `session_worker_path` or dogfood startup plumbing without adding a second runtime mode.
   - If the core daemon returns a structured spawn error and hub flattens it, preserve a stable bounded classification and sanitized message through `HubClientError` and daemon DTOs.
   - If session-id reuse is the root cause, stop before choosing behavior and ask a blocking human question: should repeated botster-web dogfood spawn reject with duplicate-session diagnostics, or should it explicitly clean up and respawn?

3. Improve diagnostics without leaking host paths.
   - Ensure Spawn failures produce `DaemonResponseKind::OperatorError` with operation `spawn`, a non-generic code, and at least one diagnostic row whose kind/message distinguishes missing worker, spawn failed, duplicate session, invalid command, or other runtime failure as far as the underlying error allows.
   - Preserve the public DTO contract from [[botster hub diagnostics use daemon diagnostic rows in client dtos]]: same-device clients should consume `DaemonResponse.diagnostics` and `DaemonStatus.diagnostics` rows before lower-detail runtime observations.
   - If adding fields or kinds in `crates/botster-hub-client`, verify mirrored web DTO field names against authoritative Rust serde structs per [[botster web dto field names must match authoritative rust serde structs]]; `DaemonDiagnostic` fields are `kind`, `operation`, `feature`, and `message`.
   - Keep request-level errors as operator frames, not transport disconnects.
   - Add negative assertions that response/error/diagnostic text does not include local user home paths.

4. Prove the browser/operator path changed.
   - Add a real-daemon smoke that sends the same request shape used by botster-web, then proves `ListSessions` includes `botster-web-dogfood-session`, `Attach` succeeds, `Drain` returns a marker, and package/entrypoint list status remains observable.
   - Add an intentional failure case, such as a command guaranteed to fail at spawn or a duplicate-session spawn, and assert the browser-consumable daemon response is actionable and not only `Runtime`.
   - Required user-path proof: inspect or test the botster-web diagnostic consumption path and document the production entry point that receives daemon responses and surfaces `DaemonDiagnostic` rows in connection diagnostics/action-failed UI. If the web checkout is available, add or run a focused web-side test using the real Rust serde field names. If it is not available in the hub worktree, document the exact consumer contract the hub provides and require downstream botster-web verification before closing acceptance.
   - If the root cause is core-owned, create/mark the core dependency ticket and keep only the hub-side diagnostic improvement plus exact evidence in this ticket.

## Risks

- Error-detail risk: exposing raw `CoreDaemonError` strings can leak local paths or unstable dependency wording. Keep diagnostics bounded and sanitized; tests should assert no home/data-dir leakage.
- False proof risk: in-process `HubClientApi` tests already pass but do not cover the failing daemon-socket packaged-web path. Acceptance must use a real local daemon and public client crate/daemon DTOs.
- Core-boundary risk: a true botster-core spawn/runtime bug should not be hidden by hub remapping. The plan requires stopping and filing a dependency ticket if hub evidence proves core ownership.
- Flaky process risk: real daemon/session-worker tests are slower and serialized. Reuse the existing `REAL_DAEMON_TEST_LOCK` pattern and keep the smoke narrowly scoped.
- Protocol drift risk: adding diagnostics in `botster-hub-client` can affect downstream clients. Prefer additive fields/kinds and preserve default serde behavior.
- Web DTO drift risk: hub can emit a good diagnostic while botster-web reads the wrong field name. Mitigation: verify web DTO mirrors against Rust serde names and make the web-consumer linkage part of acceptance.
- Session-id reuse risk: dogfood may fail because `botster-web-dogfood-session` already exists from a previous action. If instrumentation proves this is the cause, the implementer must ask a human which contract is intended before coding behavior: reject with a clear duplicate-session diagnostic, or perform explicit cleanup before re-spawn.
- Root-cause prioritization risk: spending time on worker-path hypotheses can waste the first pass because dogfood startup already proves a successful worker-backed Spawn. Start with command/session-id/bridge mapping and keep worker path as a checked but lower-priority fallback.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test <new_or_updated_botster_web_spawn_smoke>` passes and proves:
  - daemon started with explicit `botster-session-worker`;
  - `DaemonRequest::Spawn` for `botster-web-dogfood-session` succeeds through the daemon socket;
  - `ListSessions` returns the running session;
  - `Attach` and `Drain` return expected marker output;
  - package/entrypoint rows remain observable for UI panel refresh;
  - shutdown/cleanup succeeds.
- `./test.sh --test hub_daemon_lifecycle_test <new_or_updated_spawn_failure_diagnostics_test>` passes and proves an intentional Spawn failure returns `operator_error` with operation `spawn`, an actionable non-generic diagnostic, no transport disconnect, and no PII/path leak.
- Required web user-path proof passes or is documented with exact consumer evidence:
  - Identify the botster-web production function/module that consumes daemon `DaemonResponse.error` and `DaemonResponse.diagnostics` for action failures.
  - Prove it renders or generically surfaces `DaemonDiagnostic` rows using the authoritative Rust JSON field names `kind`, `operation`, `feature`, and `message`.
  - If botster-web is outside this worktree and unavailable to test, implementation must include the exact hub DTO evidence plus a blocking downstream verification/dependency record rather than treating manual smoke as optional.
- Existing focused tests still pass:
  - `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`
  - `./test.sh --test hub_daemon_lifecycle_test cli_request_level_runtime_error_returns_operator_frame_and_keeps_daemon_responsive`
  - `./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown`
- If public DTOs or docs change, run the relevant client crate/doc checks through the repo test wrapper where possible, and run clippy only if the implementation touches lint-sensitive shared Rust surfaces.
- Manual real botster-web smoke remains useful evidence, but it is no longer the only web-path proof: if the real botster-web checkout/package is available, run `botster-hub dogfood --web-package-path <botster-web>` with a real local daemon, press the packaged spawn action, confirm the session/package panels update, and confirm failures show the new bounded diagnostic.

## Pipeline gates and artifacts

- This file is the durable Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Plan gate evidence should include this artifact path, loaded context, the checklist timeout fallback, convention conflict result, assumptions/unknowns, affected files, risks, acceptance checks, and vault gap decision.
- Implement evidence must identify the production entry point it changed. Code existence is insufficient; the report must show the request path from public daemon client/browser bridge to `HubRuntime`/`CoreDaemon`.

## Convention conflicts

None found. The plan follows the loaded Botster constraints: hub remains the first-party host profile over core, local clients use `HubClientApi`/`HubRuntime` over the daemon protocol, product workflow stays in package/dogfood surfaces, session mechanics stay in core/session-worker unless proven core-owned, diagnostics derive from runtime signals, public clients consume daemon diagnostic rows from DTOs, web mirrors must match Rust serde field names, and checklist evidence is preserved in durable plan/gate/checklist surfaces.

## Vault gaps worth capturing

- Capture a new note if the implementation identifies a durable Spawn diagnostic taxonomy, for example a stable rule for preserving core spawn failure causes across `HubClientError` and `DaemonDiagnostic`.
- Capture a new gotcha if the root cause is session-id reuse by botster-web dogfood actions; that would be a recurring product/runtime contract issue.
- Capture a core dependency note if the exact request fails inside botster-core/session-worker after hub request construction and worker-path setup are proven correct.
- The checklist timeout recurred in this run; the existing [[project pipelines checklist worker timeouts require artifact evidence fallback]] note already covers the fallback, so no new capture is needed unless additional failure mode details emerge.
