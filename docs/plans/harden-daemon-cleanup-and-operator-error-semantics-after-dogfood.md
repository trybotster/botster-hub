---
description: Plan for hardening daemon-backed session cleanup and structured operator errors after local dogfood.
---

# Harden daemon cleanup and operator error semantics after dogfood

## Context loaded

- Pipeline context loaded for ticket `ticket_1780616379_314373`, run `run_1780616467_557478`, step `botster_plan`, gate `botster_plan_gate`. There are no prior artifacts, findings, reviews, questions, or answers.
- Vault/playbook context loaded: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], and [[plan steps need reviewable plan artifacts]].
- Repo context inspected: `src/main.rs`, `src/daemon_transport.rs`, `src/daemon.rs`, `src/client_api.rs`, `src/runtime.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_local_dogfood_test.rs`, `README.md`, `Cargo.toml`, and `test.sh`.
- Checklist workflow note: `project_pipelines_create_vault_checklist` timed out twice in the plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan and gate evidence carry the vault/context/checklist provenance directly.

## Scope

- Harden the daemon socket request/response boundary so request-level runtime/client failures are serialized as structured operator responses instead of propagating as `DaemonTransportError` from `handle_connection` and closing the client socket.
- Make `sessions shutdown` deterministic for normal cleanup races:
  - already exited or naturally finished sessions should return an idempotent cleanup response;
  - unknown sessions and startup-marked stale sessions should return a structured operator response, not `client disconnected`;
  - live sessions should still route through `HubClientApi -> HubRuntime -> CoreDaemon`.
- Keep the CLI thin: `sessions` commands still frame daemon transport requests and render operator-safe key/value output. The production path must remain `src/main.rs -> daemon_transport_request/stream_attach -> serve_daemon -> HubClientApi -> HubRuntime`.
- Add regression coverage for the manual dogfood sequence: start daemon, spawn a short-lived session, attach/drain output containing `dogfood-ok`, let the process exit or become unavailable, run `sessions shutdown`, and assert a deterministic response without `client disconnected`.
- Clarify package command behavior while touching docs: package commands currently mutate durable hub state in a short-lived hub-policy path and the already-running session daemon snapshots package registry state at startup. Unless implementation chooses a small daemon-routed reconciliation path, document that package changes are visible to a running daemon only after restart.

## Non-scope

- No reintroduction of in-process PTY ownership, CLI shell-out parsing, or raw core command routing.
- No browser, TUI, Rails, Project Pipelines UI, plugin worker, ActionCable, WebRTC, package marketplace, provider supervision, or package lifecycle runtime expansion.
- No broad package-command redesign unless a minimal daemon-routed package reconciliation is demonstrably smaller than documenting the current split.
- No new abstraction layer beyond the response/error vocabulary needed to keep the daemon transport open and operator output deterministic.

## Assumptions and unknowns

- Assumption: core daemon shutdown errors for missing/exited/stale sessions are distinguishable enough at the hub boundary by current `CoreDaemonError` display/debug text or by existing session registry state. If they are not, implementation should add the narrowest hub-side classification possible and avoid changing core unless required.
- Assumption: idempotent `sessions shutdown` can be a successful structured response for already-exited cleanup races, while truly invalid operations can be a structured operator error. Review should verify the final semantics are explicit in CLI output and tests.
- Unknown: whether the upstream core daemon marks naturally exited sessions as `Exited` quickly enough for `sessions shutdown` to short-circuit from `list_sessions`, or whether the regression needs to poll list/drain until the exit is recorded.
- Unknown: whether `stream_attach` always observes `ProcessExit` for the short-lived command on every test platform. The test should poll readiness/output and avoid fixed sleeps where possible.
- No human question is needed at plan time because the ticket allows either idempotent structured success or structured operator error for cleanup races, and it explicitly allows documentation for the package split.

## Botster layers touched

- Rust hub daemon transport: request framing, response/error serialization, connection lifecycle.
- Rust hub local client API/runtime facade: shutdown classification and operator-safe error semantics.
- Rust CLI operator surface: `sessions shutdown` and generic daemon response rendering.
- Docs: local dogfood operator CLI package/session split in `README.md`.
- Tests: Unix daemon lifecycle integration tests and possibly local dogfood test helpers.

## Affected surfaces/files

- `src/daemon_transport.rs`
  - Add a structured daemon operator error/response shape, for example an `operator_error` field or `DaemonResponseKind::OperatorError`.
  - Change control-message handling so `HubClientError`/`HubRuntimeError` request failures are converted to response frames when the daemon is still alive.
  - Ensure only real transport failures, protocol errors, or daemon shutdown close the socket.
  - Consider idempotent `ShutdownSession` classification by consulting current session list before or after core shutdown.
- `src/client_api.rs`
  - Preserve operation/request ids in runtime errors; add narrow error detail/classification if needed for shutdown semantics.
- `src/runtime.rs`
  - If needed, add a small helper around `shutdown_session` or `session` so callers can classify `Running`, `Exited`, `Failed/stale`, and missing sessions without reaching into core internals.
- `src/main.rs`
  - Render structured operator responses without printing generic `botster-hub sessions error: client disconnected` for request-level cleanup races.
  - Keep top-level `shutdown` behavior unchanged and still printing `response=shutdown`.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add the daemon-backed CLI regression for short-lived session attach/drain then `sessions shutdown`.
  - Add direct transport coverage that a request-level runtime error returns a structured frame and leaves the daemon responsive for a later `status` or top-level `shutdown`.
- `tests/hub_local_dogfood_test.rs`
  - Update only if the transport-level test needs shared helper extraction.
- `README.md`
  - Clarify running-daemon visibility for package mutations unless implementation routes package operations through the daemon.

## Risks

- Treating every runtime error as idempotent success would hide real operator failures. Classification should be limited to cleanup races that the ticket calls normal.
- Returning structured error frames but leaving `operator_sessions` with a zero exit status for real failures would weaken CLI semantics. Tests should assert both stdout/stderr content and process status where relevant.
- Mapping upstream core errors by formatted text may be brittle. Prefer current session state inspection first; use string matching only as a last resort and capture a vault gap if that is necessary.
- `stream_attach` has an idle-window exit path; the regression must prove `dogfood-ok` was actually streamed before shutdown, not merely that attach returned.
- Concurrent package commands against a running daemon can create operator confusion if docs are vague. The plan should either make the daemon observe those changes or state the restart visibility boundary plainly.
- Avoid path/PII leakage in test output and plan/docs; assertions should check that data-dir/package paths are not printed where existing tests already enforce path-neutral output.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test <new_short_lived_session_shutdown_test_name>`
  - Starts `botster-hub start --data-dir`.
  - Runs `sessions spawn --session-id dogfood-session -- "printf 'dogfood-ok\n'"`.
  - Runs `sessions attach ... dogfood-session` and asserts stdout contains `dogfood-ok`.
  - Runs `sessions shutdown ... dogfood-session` after natural exit and asserts deterministic structured output, no `client disconnected`, and the expected exit status.
  - Runs top-level `shutdown --data-dir` and asserts daemon process exits cleanly.
- `./test.sh --test hub_daemon_lifecycle_test <new_structured_request_error_test_name>`
  - Sends an invalid/missing-session daemon request and asserts a structured operator error frame, then sends `Status` or top-level shutdown to prove the daemon socket remains usable.
- Existing focused coverage:
  - `./test.sh --test hub_daemon_lifecycle_test`
  - `./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown`
- If docs/package split is touched:
  - `rg -n "client disconnected|Package commands are a separate|visible.*restart|running daemon" README.md src tests` should show no stale claim that package mutations are live inside an already-running daemon unless implementation proves that behavior.
- Final verification should include `cargo fmt` and the repo's strict Rust check path if available for this crate. At minimum run `./test.sh --test hub_daemon_lifecycle_test` after focused tests pass.

## Runtime path proof required

Implementation evidence must show the changed behavior is used by the real operator path:

- CLI path: `src/main.rs` `operator_sessions` for `SessionAction::Shutdown`.
- Transport path: `daemon_transport_request` writes `DaemonRequest::ShutdownSession`; `handle_connection` receives a framed response rather than socket EOF; `handle_control_request` routes through `HubClientApi`.
- Runtime path: `HubClientApi::handle_request` still calls `HubRuntime::shutdown_session`, which still delegates to core daemon session-worker ownership.

Evidence that a response type exists is not enough; tests must exercise the compiled `botster-hub` binary or `daemon_transport_request` against a live `serve_daemon` path.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Gate evidence should include this plan path, loaded context, explicit assumptions, affected files, risks, tests, and the checklist timeout fallback.
- Downstream implementation should report changed behavior separately from any merge/test cleanup and should attach exact command evidence.

## Vault gaps worth capturing

- Capture if implementation must classify upstream `CoreDaemonError` by formatted string because no stable missing/exited/stale shutdown error vocabulary exists.
- Capture if the package-command split remains documented-only and dogfood keeps showing operator confusion around "durable state changed but running daemon package registry did not."
- Capture if `stream_attach` lacks a stable process-exit signal for naturally finished short-lived sessions and tests need idle-window workarounds.
- No new vault note is required at plan time; existing notes cover the daemon transport boundary, client-worker/session-worker ownership, package registry persistence, checklist timeout fallback, and plan artifact hygiene.
