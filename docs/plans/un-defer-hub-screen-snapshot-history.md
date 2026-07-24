# Hub Screen Snapshot And History Exposure Plan

## Context Loaded

- Pipeline context: `ticket_1783552997_403516`, run `run_1783624340_265644`, Plan step `run_step_1783624340_782707`, gate `botster_plan_gate`.
- Dependencies listed by Project Pipelines are closed: Core daemon exposure ticket and Core daemon durability ticket.
- Vault/playbook context loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[coredaemon must expose terminal truth used by the production hub path]], [[botster data plane bypasses the hub through session and client actors]], [[stale project pipeline worktrees can miss merged dependency apis]], and [[botster local client api lives over hubruntime not raw core routers]].
- Repo context checked: `README.md`, `src/lib.rs`, `src/runtime.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, `tests/hub_client_api_test.rs`, and current `Cargo.lock`.
- Checklist note: `project_pipelines_create_vault_checklist` timed out with `plugin worker invoke timeout`; per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and gate evidence.

## Scope

- Run `cargo update -p botster-core -p botster-core-daemon` to move the stale lockfile pin from `978c436865c215828b02a8b0fcca5f8d89413e96` to core main `b0f8b8e` or later, where the closed dependency work exposes `CoreDaemon::read_screen(ReadScreenRequest) -> ReadScreenResult` and `CoreDaemon::capture_snapshot(CaptureSnapshotRequest) -> CaptureSnapshotResult`.
- Wire `HubRuntime` to those CoreDaemon-backed screen and snapshot APIs, using explicit hub facade methods rather than exposing core's generic command router.
- Replace `HubClientApi`'s current `UnsupportedDaemonOperation` responses for `ReadScreen` and `CaptureSnapshot` with calls through `HubRuntime`.
- Return readback through typed request/response DTOs distinct from the drain event stream: `ReadScreen` should expose the plain screen result, and `CaptureSnapshot` should expose snapshot metadata plus the smallest client-safe payload shape agreed from `CaptureSnapshotResult`. Do not overload `HubClientEvent::Snapshot`, which is reserved for subscription/drain egress.
- Expose the same operations through the daemon socket transport by extending `botster-hub-client::DaemonRequest`, generated TypeScript, request mapping in `src/daemon_transport.rs`, and docs.
- Preserve late-attach history delivery through the existing attach/drain path. History arrives asynchronously from the real session worker as `SessionIoEvent::InitialSnapshotReady`, then reaches clients through `TransportEgress::Snapshot`/`Scrollback` mapped by `events_from_drain`.
- Split the combined facade decision table row in `src/lib.rs` and `README.md`: mark `read_screen/capture_snapshot` as Exposed, and leave `report_delivery_*` Deferred because CoreDaemon main exposes no `report_delivery_*` method.
- Update README/client protocol documentation for client-facing request types.

## Non-Scope

- No full web/TUI renderer polish.
- No Ghostty packaging or binary discovery beyond the existing session-worker path.
- No reintroduction of `DefaultBotsterEngine` as the hub production engine.
- No broad rewrite of `HubRuntime`, `HubClientApi`, daemon transport framing, package policy, or client event semantics.
- No speculative compatibility layer or dual protocol version unless an actual external boundary requires it.

## Assumptions And Unknowns

- Determined fact: the dependency API has landed upstream. The hub lockfile is stale; update `botster-core` and `botster-core-daemon` before implementation and code against `ReadScreenRequest`/`ReadScreenResult` and `CaptureSnapshotRequest`/`CaptureSnapshotResult`.
- Determined fact: CoreDaemon main exposes no `report_delivery_*` method, so `report_delivery_*` remains Deferred and must not be claimed as Exposed.
- Assumption: readback remains a control-plane request/response. Terminal history and per-client egress stay on the SessionIo/ClientWorker data-plane path; hub must not become a general bulk byte relay.
- Assumption: late attach history acceptance requires a real session worker path. Tests that construct CoreDaemon without `HubConfig.core_engine.session_worker_path` can pass vacuously with empty history and may fail after the lock bump brings in the CoreDaemon durability fail-loud change.
- Unknown: final client boundary for the opaque snapshot payload. The plan chooses distinct response DTOs; implementation must decide whether the opaque payload crosses the client boundary or is summarized/metadata-only, consistent with [[botster data plane bypasses the hub through session and client actors]].

## Affected Surfaces And Files

- Rust hub runtime facade: `src/runtime.rs`.
- Local client API: `src/client_api.rs`.
- Daemon socket adapter and request mapping: `src/daemon_transport.rs`.
- External client protocol crate and generated DTOs: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, likely `crates/botster-hub-client/src/typescript.rs`.
- Architecture facade audit: `src/lib.rs`.
- Docs: `README.md`, likely `docs/client-protocol.md`.
- Tests: `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs` or another daemon transport integration test, and hub-client protocol serde/generation tests in `crates/botster-hub-client/src/lib.rs`.
- Read for precedent, not expected to change: `tests/support/mod.rs` (`ensure_session_worker_binary`, already called by both test files above) and `tests/hub_local_runtime_test.rs` (`spawn_attach_input_and_drain`, the existing worker-backed spawn/attach/drain flow through `HubClientApi`).

## Implementation Shape

1. Run `cargo update -p botster-core -p botster-core-daemon` so the hub consumes core main `b0f8b8e` or later. Do not use `DefaultBotsterEngine` directly from hub production code.
2. Add explicit `HubRuntime::read_screen` and `HubRuntime::capture_snapshot` methods that lock `core_daemon` and delegate to CoreDaemon.
3. Update `HubClientApi::handle_request` branches for `ReadScreen` and `CaptureSnapshot` to return typed success response bodies, not drain `HubClientEvent` variants.
4. Add daemon request variants if absent, map them to `HubClientRequest`, and project `HubClientResponseBody` back to `DaemonResponse`.
5. Regenerate or update TypeScript daemon protocol output from the authoritative Rust serde structs.
6. Split the facade audit row in code/docs: `read_screen/capture_snapshot` Exposed, `report_delivery_*` Deferred. Update assertions at `src/lib.rs:413-414` accordingly.
7. Check hub tests that construct CoreDaemon without a worker path after the lock bump, because the same update brings in fail-loud behavior for claimed durability without a worker path.
8. Add integration coverage for spawn/attach/drain late history and screen/snapshot request through the public hub API/daemon transport.

## Risks

- False exposure risk: changing docs/table without routing production requests through `daemon_transport -> HubClientApi -> HubRuntime -> CoreDaemon`.
- Lockfile hygiene risk: the `cargo update` must be committed. `tests/support/mod.rs` builds the session worker with `cargo build --locked`, so an uncommitted or inconsistent `Cargo.lock` fails the worker fixture and every worker-backed test.
- History ordering risk: late attach must preserve non-empty initial history before later live bytes; tests must assert presence and order.
- Readback side-effect risk: CoreDaemon readback methods take mutable daemon access and drain runtime state before readback. A `ReadScreen` or `CaptureSnapshot` between attach and first drain can perturb the pending-drain queue that feeds late-attach history.
- DTO drift risk: Rust serde structs and generated TypeScript must stay aligned.
- Lock-bump blast radius: the required core update also brings in fail-loud behavior when CoreDaemon durability is claimed without a worker path, so existing hub tests may need explicit worker-path setup.
- Overreach risk: pulling in old embedded engine methods would violate the ticket and local architecture notes.

## Acceptance Checks And Tests

- `./test.sh hub_client_api_test`
- `./test.sh hub_daemon_lifecycle_test` or a more focused daemon transport test name that exercises the new daemon request variants.
- `cargo test -p botster-hub-client` for serde/TypeScript protocol drift tests.
- A focused integration proves: call `support::ensure_session_worker_binary()` and let `session_worker_path` discovery resolve the worker (no explicit `session_worker_path` config needed), spawn a session, attach once, produce output, attach a late subscription through the hub API/daemon path, drain, and observe NON-EMPTY renderable history strictly before the first live output. Follow the existing worker-backed precedent in `tests/hub_local_runtime_test.rs:228` `spawn_attach_input_and_drain`; do not hand-roll a second worker-config path.
- A focused client API test proves `ReadScreen` and `CaptureSnapshot` no longer return `UnsupportedDaemonOperation`.
- A regression test proves `ReadScreen` or `CaptureSnapshot` issued between attach and first drain does not drop or reorder retained history relative to live output.
- Docs/assertions prove `architecture_summary().facade_decisions()` marks `read_screen/capture_snapshot` as `Exposed` and `report_delivery_*` as still `Deferred`.
- Existing hub tests that construct CoreDaemon are checked under the bumped core dependency for the worker-path fail-loud behavior.

## Vault Gaps Worth Capturing

- Capture a Botster note if the implementation settles a durable rule for stale lockfile pins after closed dependency tickets: planners should distinguish stale lockfile pins from stale run worktrees.
- Capture the final client DTO rule for `CaptureSnapshotResult { snapshot, payload }`, especially whether the opaque payload crosses the client boundary.
- No convention conflict found. The plan follows the loaded Botster constraints: local clients use `HubRuntime`/`HubClientApi`, hub stays a host profile over CoreDaemon, and terminal data/history stays on the SessionIo/ClientWorker/CoreDaemon path.
