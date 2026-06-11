---
description: Plan for proving dogfood packaged bridge package/session/status DTO consistency.
---

# Prove dogfood bridge package and session DTO consistency

## Context loaded

- Project Pipelines context loaded with `project_pipelines_current_context` for ticket `ticket_1781136766_587872`, run `run_1781136806_296771`, run step `run_step_1781136806_686601`, current step `botster_plan`, gate `botster_plan_gate`.
- Pipeline state: no prior artifacts, findings, reviews, open questions, or prior answers are present for this run.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo context inspected: `docs/plans/*`, `src/main.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `tests/hub_local_dogfood_test.rs`.
- `origin/main` check: after `git fetch origin`, `git grep` against `origin/main` found daemon/client DTOs and the same `tests/hub_daemon_lifecycle_test.rs` fixture bridge, but no in-repo real botster-web bridge request endpoint/envelope. The real botster-web bridge remains outside this repo, so any self-authored fixture envelope is not production-bridge proof.
- DTO context loaded from [[botster web dto field names must match authoritative rust serde structs]]: bridge/package/session/diagnostic assertions must use authoritative `botster-hub-client` serde field names, not fixture-invented browser shapes.
- Project Pipelines checklist discipline: loaded checklist instructions. `project_pipelines_create_vault_checklist` initially returned a plugin worker timeout, but the checklist was later visible as `checklist_1781136872_691786` and updated with vault/context and convention evidence. This plan and gate evidence also preserve the same evidence directly.

## Scope

- Strengthen Botster hub integration coverage around `botster-hub dogfood --web-package-path ...` and the packaged `botster-web` dogfood bridge fixture.
- Prove the hub-side consistency claim that `botster-hub dogfood` passes the correct socket/data-dir/port to the supervised entrypoint and that the daemon returns consistent `Status`, `ListPackages`, `ListSessions`, and terminal action DTOs for that same data dir.
- Drive these operations through the packaged bridge endpoint against the dogfood-started daemon: `status`, `list_packages`, `list_sessions`, `spawn`, `attach` plus terminal output drain or stream, `send_input`, `resize`, process-exit/lifecycle visibility, and `shutdown`.
- Assert `list_packages` includes `project-pipelines` and `botster-web` as enabled when dogfood output has printed those packages as enabled for the same data dir.
- Assert `list_sessions` reflects spawned, running, and exited/shutdown session state consistently with daemon/CLI visibility.
- Split the real-bridge correctness claim from the hub-side proof: a self-authored fixture that forwards to `BOTSTER_HUB_SOCKET` can prove daemon/launcher/env wiring, but it cannot prove that the real botster-web bridge will not produce `No installed packages`.
- Treat real-bridge proof as a required decision point. If the implementer cannot reuse the real botster-web request/response envelope in this repo, they must stop and add or mark a botster-web dependency with exact request/response evidence instead of declaring the `No installed packages` acceptance satisfied.
- Assert known operator diagnostics against exact public DTO fields: `DaemonOperatorError.code`, `DaemonOperatorError.operation`, and `DaemonOperatorError.message`, plus nested `DaemonDiagnostic.kind`, `DaemonDiagnostic.operation`, `DaemonDiagnostic.feature`, and `DaemonDiagnostic.message` where diagnostics are present.
- Keep work in `botster-hub` unless implementation captures exact botster-web request/response evidence proving the real package sends malformed requests.

## Non-scope

- No botster-web repo changes unless the hub-side proof exposes a malformed real-package request contract and the implementer records exact evidence.
- No UI polish, browser rendering changes, or Project Pipelines surface work.
- No new daemon protocol, alternate bridge transport, package registry redesign, or broad dogfood launcher refactor.
- No fake hub/session-worker path for acceptance coverage; tests must exercise real subprocess daemon/core/session-worker behavior.
- No raw local path or PII in dogfood output, bridge diagnostics, or plan/gate artifacts.

## Assumptions and unknowns

- Assumption: the hub-side bridge fixture can be used only as daemon-consistency proof if it is self-authored. It must not be treated as proof of the real botster-web bridge's request mapping.
- Assumption: `BOTSTER_HUB_SOCKET`, `BOTSTER_HUB_DATA_DIR`, and `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT` are the authoritative runtime inputs for same-daemon proof because `src/main.rs` passes them to `StartPackageEntrypoint`.
- Assumption: `bridge=` remains the API/diagnostic bridge URL while `web=` is the verified browser HTML URL; this ticket focuses on the API bridge request path behind that same supervised package entrypoint.
- Assumption: the implementation should begin by adding failing integration coverage. Production changes should be surgical and only follow if the new bridge-path test exposes a real mismatch.
- Decision point: if the real botster-web HTTP request path/envelope cannot be imported, vendored, or otherwise exercised in `botster-hub`, the implementation must not claim "`No installed packages` cannot be produced by the bridge." It must stop and create/mark a botster-web dependency with exact request and response evidence, as the ticket directs.
- Unknown: the exact real botster-web bridge HTTP path and JSON envelope are not present in this repo. If implementation cannot infer the contract from current package behavior or existing docs, it should ask a human or add a botster-web dependency with exact evidence rather than silently inventing a new public shape.
- Unknown: terminal event streaming may need a bounded request/response drain loop in the bridge fixture rather than a long-held HTTP stream if the actual package bridge currently exposes request-based actions. The acceptance criterion is real session-backed terminal events through the bridge contract, not a specific fixture implementation detail.
- Diagnostic DTO grounding: `crates/botster-hub-client/src/lib.rs` currently defines `DaemonOperatorError { code, request_id, operation, message, diagnostics }` and `DaemonDiagnostic { kind, operation, feature, message }`. `DaemonDiagnosticKind` variants are `Connected`, `Disconnected`, `CompatibilityMismatch`, `UnsupportedFeature`, `TerminalStreamUnavailable`, `ActionFailure`, and `DaemonStartupFailure`. There is no `code` field on `DaemonDiagnostic`, no `kind` field on `DaemonOperatorError`, and no daemon diagnostic variant named `Runtime`.

## Botster layers touched

- Rust hub CLI/operator path: `botster-hub dogfood` launch output, data-dir/socket ownership, and package enablement proof.
- Rust hub daemon transport and public client DTOs: `Status`, `ListPackages`, `ListSessions`, `Spawn`, `Attach`, `Drain`, `SendInput`, `Resize`, and `ShutdownSession`.
- Packaged bridge fixture inside hub integration tests: synthetic `botster-web` package script that should proxy bridge HTTP requests to the dogfood daemon socket.
- Session/client worker runtime: exercised only through real daemon protocol and session-worker subprocesses.
- No SPA, Rails relay, TUI, or Lua plugin policy changes are planned unless evidence narrows a failure there.

## Worktree and target assumptions

- Current pipeline run is bound to target id `tgt_7e208a0c76a44980a83b63af976b1f22` and this run worktree.
- Downstream agents must work in their assigned run worktree, not an ambient checkout.
- Plan artifacts should use vault note wikilinks and repo-relative paths, not raw local home paths.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle_test.rs`
  - Extend `write_botster_web_package()` so the fixture bridge exposes the packaged HTTP request endpoint and forwards requests to `BOTSTER_HUB_SOCKET`.
  - Add a dogfood launcher integration test that starts the real foreground dogfood command, reads the printed data dir/package/bridge lines, then calls the bridge endpoint for status, packages, sessions, spawn, attach/drain or terminal stream, send input, resize, process exit, and shutdown.
  - Cross-check bridge results against `botster-hub status`, `botster-hub packages list`, and/or direct daemon transport for the same explicit data dir where needed.
  - Add or strengthen known-failure diagnostics assertions for bounded code/operation/message fields.
- `src/main.rs`
  - Expected no change unless the bridge-path test reveals dogfood starts the package with the wrong socket/data-dir/port or prints package state before it is authoritative.
- `src/daemon_transport.rs` and `crates/botster-hub-client/src/lib.rs`
  - Expected no change unless DTO serialization/diagnostics are inconsistent under the bridge path.
- `crates/botster-hub-test-support/src/lib.rs`
  - Optional only if an existing helper can reduce duplicate real-daemon conformance code without hiding the packaged bridge path. Do not move the bridge proof into a helper that stops exercising `botster-hub dogfood`.
- `docs/plans/prove-dogfood-bridge-package-session-dto-consistency.md`
  - This plan artifact.

## Risks

- A test that calls `botster_hub_client` directly proves the daemon DTOs but not the packaged HTTP bridge. At least one acceptance test must enter through the bridge URL printed by dogfood.
- A fixture-only request envelope can drift from real botster-web. If the real envelope is not knowable in this repo, stop and ask or create a botster-web dependency with exact evidence.
- A faithfully forwarding fixture will always return enabled packages after dogfood enables them, so it cannot reproduce the production `No installed packages` symptom by construction. It proves hub/daemon consistency only, not the real web bridge DTO mapping.
- Per-request HTTP attach can accidentally detach terminal subscriptions too early. The test must prove terminal output after attach/drain or a held stream, not only an `Events` response to `Attach`.
- Package registry assertions can race entrypoint startup. Use the already printed dogfood readiness lines and daemon package state after readiness as the synchronization point.
- Session lifecycle assertions can be flaky if the spawned command exits before list checks. Use a deterministic shell command that prints ready, echoes input, reports `stty size`, then exits only after an explicit quit.
- Operator diagnostics can become overfit to prose or the wrong DTO. Assert `DaemonOperatorError.code/operation/message` and, when present, nested `DaemonDiagnostic.kind/operation/feature/message`; do not assert nonexistent `DaemonDiagnostic.code` or `DaemonOperatorError.kind`.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_bridge_request_endpoint_uses_same_daemon_state`
  - New or final equivalent test name.
  - Starts `botster-hub dogfood --web-package-path ...` with an isolated data dir and packaged bridge fixture.
  - Confirms dogfood output reports `project-pipelines` and `botster-web` enabled, a bridge URL, a web URL, and the same data dir used by CLI checks.
  - Proves hub-side daemon consistency: the supervised bridge fixture receives the dogfood-provided `BOTSTER_HUB_SOCKET`, `BOTSTER_HUB_DATA_DIR`, and `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT`, then the daemon returns consistent status/package/session/terminal DTOs for that same data dir.
  - Calls the bridge request endpoint for `status` and proves lifecycle/running status matches the dogfood daemon.
  - Calls `list_packages` through the bridge and proves enabled `project-pipelines` and `botster-web` rows are present.
  - Calls `list_sessions`, `spawn`, `attach` plus drain or terminal stream, `send_input`, `resize`, and `shutdown` through the bridge and proves session lifecycle/output/resize behavior.
  - Cross-checks `list_sessions` after spawn and after process exit/shutdown against the daemon/CLI for the same data dir.
- Real-bridge decision gate
  - If implementation uses a self-authored bridge fixture envelope, mark the `No installed packages cannot be produced by the bridge` acceptance criterion as not satisfied by hub-only evidence.
  - To satisfy that criterion, implementation must exercise the real botster-web request/response envelope against the dogfood-started daemon or add/mark a botster-web dependency with exact request payload, response payload, and mismatch evidence.
  - The plan intentionally does not waive this criterion; it separates what this repo can prove from what requires the real bridge.
- Diagnostic DTO assertion gate
  - For known dogfood/bridge failure states, assert `DaemonOperatorError.code`, `operation`, and `message` are bounded and specific.
  - If response diagnostics are present, assert `DaemonDiagnostic.kind` is one of the bounded `DaemonDiagnosticKind` variants and that `operation`, `feature`, and `message` use the current serde field names.
  - Do not assert a generic `Runtime` diagnostic kind because that is not an authoritative `DaemonDiagnosticKind`; if a generic runtime collapse exists, map it to the actual observed `DaemonOperatorError.code` or the bridge/browser DTO evidence.
- `./test.sh --test hub_daemon_lifecycle_test external_hub_client_spawns_botster_web_dogfood_session_request_shape`
  - Preserve or update as lower-level public-client DTO coverage; it is not sufficient by itself for this ticket.
- `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`
  - Preserve existing dogfood readiness/package/web URL/process cleanup coverage.
- `./test.sh --test hub_daemon_lifecycle_test`
  - Final focused suite if runtime permits, because the change touches the daemon lifecycle/dogfood harness.
- `cargo fmt`
  - Formatting after Rust test or production edits.

## Runtime path proof required

Implementation evidence must show the real user path changed or was proven:

- `botster-hub dogfood --web-package-path <fixture>` starts an isolated daemon/session-worker and enables the dogfood packages.
- The supervised package bridge receives `BOTSTER_HUB_SOCKET`, `BOTSTER_HUB_DATA_DIR`, and `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT` from the dogfood launcher.
- The HTTP bridge endpoint forwards operations to the daemon socket for that same data dir.
- Bridge-observed packages, sessions, terminal events, input, resize, and shutdown match daemon/CLI observations.
- If the bridge endpoint is a self-authored test fixture, the proof stops at daemon consistency and env wiring. Real botster-web DTO correctness still requires real bridge-envelope evidence or a botster-web dependency.

Evidence that DTO code exists is not enough; at least one test must enter through the dogfood-printed bridge URL.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/prove-dogfood-bridge-package-session-dto-consistency.md`.
- Plan gate evidence should include this file path plus loaded context, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Checklist: `checklist_1781136872_691786` records vault/context and convention evidence; this plan mirrors the same evidence for review durability.

## Vault gaps worth capturing

- Capture if implementation settles the packaged bridge request envelope as a durable hub/botster-web contract.
- Capture if a durable test pattern emerges for HTTP bridge fixtures that must hold terminal subscriptions open instead of using one-shot request sockets.
- Capture if operator diagnostic field assertions become a reusable rule for known dogfood failure states, especially the exact split between `DaemonOperatorError` and `DaemonDiagnostic` fields.
- No durable vault note was written at Plan time because implementation will determine whether these are ticket-local facts or standing conventions.

## Checklist evidence fallback

- Vault/context evidence: notes listed in `Context loaded` constrained the plan to Botster Rust hub/client/session-worker boundaries, repo-visible plan artifacts, explicit worktree/target assumptions, and bridge proof through the production dogfood path.
- Convention-conflict evidence: no convention conflicts found after revision. The original plan overclaimed what a self-authored fixture could prove; this revision separates in-repo daemon-consistency proof from real botster-web bridge correctness.
- Verification evidence gathered during planning: repository inspection found existing dogfood readiness tests, existing lower-level external hub client DTO tests, public daemon DTOs in `botster-hub-client`, and current dogfood launch code passing socket/data-dir/port into the supervised package entrypoint. `git fetch origin` plus `git grep` against `origin/main` confirmed the in-repo bridge remains only the test fixture and that no real botster-web request endpoint/envelope exists in this repo.
- Capture evidence: no vault capture at planning; capture after implementation only if a durable bridge contract or diagnostic convention is proven.
