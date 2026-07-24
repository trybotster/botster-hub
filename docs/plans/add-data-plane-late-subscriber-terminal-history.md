# Add Data-Plane Late-Subscriber Terminal History Primitive

Ticket: `ticket_1782163713_316845`
Run: `run_1782163723_510939`
Step: Plan
Worktree: `project-pipelines/ticket_1782163713_316845`

## Context Loaded

- Pipeline context: ticket, run, current step, gate prompt, artifacts, findings, questions, prior answers, events, review feedback, and return-to-Plan state via `project_pipelines_current_context`.
- Plan Review return: changes required because the original plan was authored on stale base `73e5dec`; this run branch has now been rebased to `origin/main` at `32a63b8`, which includes PR #62 / `d112ab5` "Expose renderable terminal history events".
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster architecture notes: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Pipeline notes: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Orchestration notes: [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Terminal data-plane notes: [[botster terminal clients share one sessionio data plane subscription path]], [[botster durable terminal egress is owned by sessionio and clientworker actors]], [[botster terminal egress is session backed only]], [[botster initial terminal scrollback is delivered by sessionio directly to clientworker]], [[terminal subscribe readiness gates on sessionio initial snapshot delivery]], [[initial terminal snapshots must precede live output activation]].
- Repo inspection after rebase: `Cargo.toml`, `Cargo.lock`, `docs/client-protocol.md`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/runtime.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`.
- Locked core inspection: the locked botster-core git checkout at rev `8f2f4ac` for `SessionIoRequest`, `SessionIoEvent`, `SessionWorkerEngine`, `ManagedSessionRuntime`, `ClientStreamHarness`, and core tests.
- Checklist discipline: `project_pipelines_create_vault_checklist` returned a plugin worker timeout, but the checklist became visible as `checklist_1782163809_272220`; four vault workflow items were added with evidence.
- Dependency state: human answer to `question_1782164358_526934` requires a registered dependency, not out-of-band core work. `ticket_1782163713_316845` now depends on `ticket_1782164631_196305` ("Add late-subscriber terminal history replay primitive to botster-core") through `dependency_1782164643_303251`; that dependency is open and blocks implementation.

## Scope

- Consume a core data-plane primitive owned by `SessionIo`/`ClientWorker` so a late terminal subscription receives renderable prior terminal history for an existing running session.
- Preserve subscription-local ordering: replayed history must be delivered before or in the same drain batch as later live `TerminalOutput` for that subscription.
- Keep the public daemon/client `Snapshot`/`Scrollback` DTO contract unchanged. On current `origin/main`, these events already carry renderable `data` plus derived `bytes`; this ticket must project replayed history into that existing contract.
- Wire the hub production path through the existing client-to-hub route: `botster_hub_client::DaemonConnection` or `stream_attach` -> `src/daemon_transport.rs` -> `HubClientApi` -> `HubRuntime` -> core `SessionIo`/`ClientWorker`.
- Add deterministic hub/data-plane coverage for late attach history replay, ordering relative to later live output, and no-history sessions.
- Update protocol/docs to explain history replay ownership and ordering.

## Non-Scope

- No `Snapshot`/`Scrollback` DTO shape work; PR #62 / `d112ab5` already added `data` and `bytes` on `origin/main`.
- No daemon_transport-owned terminal history cache, synthetic daemon-only `Scrollback`, or duplicate terminal byte store.
- No broad refactor of daemon requests, session lifecycle, Project Pipelines, web UI, TUI UI, plugin surfaces, or package/runtime plumbing.
- No new optional configurability, retention policy UI, or product workflow primitive.
- No fabrication of history for sessions with no recorded terminal output.

## Assumptions And Unknowns

- Assumption: this hub run must build on `origin/main` at or after `32a63b8`; implementation must not be attempted from stale base `73e5dec`.
- Assumption: `botster-core` is the owning implementation layer for the missing primitive. This repo depends on `botster-core` and `botster-core-daemon` from git `main`, locked at `8f2f4acf7e9b73f4ff777151bfb55284871f8bdd`.
- Confirmed: current hub DTOs already expose renderable `Snapshot`/`Scrollback` data. `HubClientEvent::Snapshot` and `HubClientEvent::Scrollback` carry `data: Vec<u8>`, daemon events carry `data: String` plus `bytes: usize`, and `daemon_transport` derives `bytes` from `data.len()`.
- Confirmed: locked core retains prior output in terminal shadow state for initial snapshots (`supervised_session_initial_snapshot_after_prior_output_reflects_shadow_state`), but `ClientStreamHarness` drops `SessionIoEvent::InitialSnapshotReady(_)` instead of routing it to `TransportEgress::Snapshot` or `Scrollback`. That is the missing data-plane late-subscriber history projection.
- Blocking dependency: `ticket_1782164631_196305` must land first. This hub ticket stays scoped to the follow-up after core lands: update the locked botster-core dependency/Cargo.lock, wire explicit Attach to the new core primitive if needed, and prove hub/docs/tests against the data-carrying `Snapshot`/`Scrollback` DTOs already on `origin/main`.
- Unknown: whether the core-side fix should project `InitialSnapshotReady` as `TransportEgress::Snapshot` or `TransportEgress::Scrollback`. Prefer the variant already used for initial attach semantics in current core/hub tests; either way the event must carry `data` and preserve `bytes == data.len()` at the daemon projection.

## Affected Surfaces And Files

- Required upstream core dependency: `botster-core` / `botster-core-daemon` `ClientStreamHarness`, `SessionIoEvent::InitialSnapshotReady`, `TransportEgress::Snapshot` / `TransportEgress::Scrollback`, subscription startup, and core tests.
- `Cargo.lock`: update the locked `botster-core` revision after the core dependency lands.
- `src/runtime.rs`: likely no code change unless the core dependency exposes a new attach option; hub should keep using the existing `CoreDaemon::attach`/`drain` path if possible.
- `src/client_api.rs`: verify existing renderable `TransportEgress::Snapshot` / `Scrollback` projection preserves event order from core drain output; only adjust if the core dependency emits a variant the hub does not currently map.
- `src/daemon_transport.rs`: verify projection into public `DaemonEvent` remains a stateless framing adapter deriving `bytes` from `data.len()`.
- `crates/botster-hub-client/src/lib.rs`: no DTO shape change expected; update only tests/docs/conformance metadata if required.
- `crates/botster-hub-test-support/src/lib.rs`: update conformance report or support matrix if late-subscriber history replay becomes part of first-party client support.
- `tests/hub_client_api_test.rs`: deterministic in-process data-plane test for late subscription history replay and ordering.
- `tests/hub_daemon_lifecycle_test.rs`: socket-level test for existing running session, late attach, prior output replay, later live output ordering, and no-history behavior.
- `docs/client-protocol.md`: document that history replay belongs to SessionIo/ClientWorker and uses the existing renderable `Snapshot`/`Scrollback` events.

## Implementation Plan

1. Do not implement this hub ticket until dependency `ticket_1782164631_196305` is closed and available to update into `Cargo.lock`.
2. Core dependency scope: route `SessionIoEvent::InitialSnapshotReady` through the `ClientWorker`/`ClientStreamHarness` path as a renderable history event for the attaching subscription, preserving the existing initial-snapshot-before-live-output barrier. Core already records prior output in the snapshot payload; the missing part is client egress projection.
3. After core lands, update `Cargo.lock` to a botster-core revision that includes the primitive.
4. Verify hub projection remains unchanged: `TransportEgress::Snapshot` / `Scrollback` -> `HubClientEvent` with `data: Vec<u8>` -> `DaemonEvent` with `data: String` and `bytes: data.len()`.
5. Keep `daemon_transport` as a framing adapter. It may serialize the event returned by `HubClientApi`, but it must not retain, reconstruct, or fabricate terminal history.
6. Add tests with deterministic shell markers:
   - spawn a running session that emits `before-late`;
   - wait until that marker is present before late attach;
   - attach with a new subscription;
   - observe `Snapshot` or `Scrollback` whose `data` contains `before-late` and whose `bytes == data.len()`;
   - send `after-late`;
   - prove replay event for that subscription appears before the later `TerminalOutput`;
   - separately attach a no-history running session and prove no fabricated history event appears.
7. Update protocol docs and, if late-history replay changes conformance visibility, bump `CONFORMANCE_FIXTURE_REVISION` and update relevant support-matrix/conformance expectations. Do not bump for DTO shape; that already landed in PR #62.

## Risks

- Core dependency risk: the needed primitive is not implementable in this hub checkout alone because `ClientStreamHarness` / `SessionIo` live in `botster-core`.
- Ordering risk: if replay is implemented after live subscription activation, live output can overtake history and violate the ticket.
- Stale-base risk: implementing from `73e5dec` would duplicate PR #62 DTO work. This run has been rebased to `32a63b8`; future agents must keep that base.
- Compatibility risk: conformance fixtures may need a revision bump if they begin asserting late-subscriber history replay.
- Flake risk: PTY output chunking is nondeterministic. Tests should accumulate events until markers are seen and assert relative event order by event sequence, not exact chunk boundaries.
- Regression risk: first attach, live streaming, input echo, resize, detach cleanup, and no-history behavior share the same path and must remain covered.

## Acceptance Checks And Tests

- Core dependency acceptance before hub implementation:
  - core test for `InitialSnapshotReady` routing to a subscribed client as renderable `Snapshot` or `Scrollback`;
  - core test preserving snapshot-before-live-output ordering for the same subscription;
  - core negative test proving no history event is emitted when the snapshot payload is empty.
- `./test.sh --test hub_client_api_test late_subscriber_terminal_history_replays_before_live_output`
- `./test.sh --test hub_daemon_lifecycle_test daemon_late_attach_receives_renderable_history_before_live_output`
- Existing focused regressions:
  - `./test.sh --test hub_client_api_test local_client_api_exercises_status_spawn_attach_input_resize_detach_shutdown_and_events`
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_client_spawns_botster_web_production runtime_session_request_shape`
  - `./test.sh --test hub_daemon_lifecycle_test daemon_socket_detaches_subscriptions_on_connection_eof`
- Broader relevant coverage before review:
  - `./test.sh --unit`
  - `cargo test --locked -p botster-hub-client`
  - `cargo test --locked -p botster-hub-test-support`
- Verification evidence must name the production entry point changed: public client attach/drain via daemon socket into `HubClientApi`, not direct private session-worker frames.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Checklist artifact: `checklist_1782163809_272220` with vault context, conflict, verification, and capture evidence.
- Dependency artifact: `dependency_1782164643_303251`, open, pointing to `ticket_1782164631_196305`.
- Answer artifact: `question_1782164358_526934`, answered with "Use a registered Project Pipelines dependency, not an out-of-band core change."
- Plan gate should include the rebase evidence, the unchanged DTO constraint, the registered core dependency, and the no-daemon-cache constraint.

## Vault Gaps Worth Capturing

- After implementation, capture a durable note if the work establishes a precise primitive shape, for example: "late-subscriber terminal history replay is a SessionIo/ClientWorker subscription primitive and must precede live output activation."
- If implementation shows late-history replay requires conformance fixture handling without a protocol version bump, capture it as a protocol evolution example tied to [[daemon event shape changes bump conformance fixture revision not protocol version]].
