# Harden hub per-client and plugin egress queue isolation diagnostics

## Context loaded

- Pipeline context: run `run_1782929496_614746`, step `botster_plan`, ticket `ticket_1782862717_670436`; no prior artifacts, findings, questions, or answers; one closed dependency: `ticket_1782857282_831231`.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Vault constraints loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], plus developer-specified [[identity]] and [[goals]].
- Relevant repo context inspected: `src/daemon_transport.rs`, `src/client_api.rs`, `src/capabilities.rs`, `src/runtime.rs`, `src/local_webrtc.rs`, `crates/botster-hub-client/src/lib.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_capability_runtime_test.rs`, `docs/client-protocol.md`, prior transport audit/report docs, and `test.sh`.
- Checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` timed out in the plugin worker, so checklist evidence must be preserved in the gate artifact per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

- Name the affected Botster layers: Rust hub daemon transport, session/client worker adapter boundary, plugin capability runtime/event queue, public daemon client DTOs, CLI/client diagnostics docs, and Rust tests.
- Audit the concrete same-device daemon path from `botster_hub_client::{request, DaemonConnection, stream_attach}` through `src/daemon_transport.rs` into `HubClientApi` and `HubRuntime`.
- Replace or wrap hub-owned shared/unbounded hot-path queues where they can let one slow daemon client block another client's terminal/control progress. The likely first target is the current per-connection synchronous response/write path plus shared `std::sync::mpsc::channel` submission into the daemon owner.
- Preserve the architectural rule that terminal bytes come from core `SessionIo`/`ClientWorker`; hub changes should classify, buffer, drop, and diagnose at the daemon/client adapter boundary rather than reconstructing terminal history.
- Add structured, PII-safe diagnostics comparable to old DeliveryKind/backpressure counters: lane/kind classification for terminal vs control, per-client drop/lag/backpressure counters, and plugin/capability queue pressure summaries.
- Surface diagnostics through existing public daemon observation/diagnostic surfaces, preferably `DaemonStatus.diagnostics`, `DaemonResponse.diagnostics`, or a narrowly additive DTO field in `botster-hub-client` if counters need structure.
- Add tests proving slow daemon clients and slow plugin/capability consumers do not stall unrelated terminal/control paths, and that diagnostics are observable and scrubbed.

## Non-scope

- Do not implement WebRTC, browser UI, or botster-web changes.
- Do not edit `botster-core` or session-worker internals unless the implementer proves the hub cannot satisfy the ticket without a dependency update and asks a human question.
- Do not add new workflow primitives, package policy, plugin UI workbench behavior, or Project Pipelines plugin behavior.
- Do not broad-refactor daemon transport, package registry, lifecycle, or CLI rendering beyond changes required by queue isolation and diagnostics.
- Do not introduce a second terminal data plane or hub-owned terminal history reconstruction.

## Assumptions and unknowns

- Assumption: the intended "clients" are daemon protocol clients using `botster-hub-client`, including CLI/TUI/local bridge consumers, not browser WebRTC clients.
- Assumption: "plugin egress" means hub-owned capability/plugin event delivery queues such as `HubCapabilityRuntime.pending_events`, HTTP/WebSocket capability events, plugin MCP/surface/action calls, and their diagnostic projection.
- Assumption: additive DTO changes are acceptable if serde defaults and generated TypeScript optionality are preserved.
- Unknown: whether `botster-core` already exposes enough structured backpressure detail to build `BackpressureSummary` without a core change. If not, keep the hub summary based on observable hub-owned queue/drop state and ask before changing core.
- Unknown: whether the old Botster `DeliveryKind::{Terminal,Control}` names should be reused exactly. Prefer semantically equivalent public names only if they fit the current daemon protocol vocabulary.
- Unknown: current `mpsc::channel` into the daemon owner may be acceptable for control requests if per-client egress is decoupled; implementation should prove the actual stall point before replacing queue mechanics.

## Affected surfaces/files

- `src/daemon_transport.rs`: daemon connection handling, per-client egress buffering, attach/drain/send/resize routing, status/diagnostic projection, PII-safe operator errors.
- `src/client_api.rs`: runtime observation mapping if structured backpressure/lag data from `BotsterEngineObservation` needs to cross the hub client boundary.
- `src/capabilities.rs`: plugin capability operation/event queue capacity, per-plugin isolation, backpressure/drop counters, event draining behavior.
- `crates/botster-hub-client/src/lib.rs`: public diagnostics/summary DTOs, serde defaults, compatibility fixture examples.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts`: only if public DTOs change.
- `src/main.rs`: only if status output must render new diagnostics; keep CLI thin.
- `docs/client-protocol.md` and possibly `README.md`: document new diagnostics and isolation guarantees if public protocol changes.
- `tests/hub_daemon_lifecycle_test.rs`: real daemon socket tests for slow-client isolation and structured diagnostics.
- `tests/hub_capability_runtime_test.rs`: plugin/capability queue isolation and backpressure/drop diagnostics tests.

## Risks

- Blocking writes to a slow Unix socket can still pin a connection thread; the fix must ensure that does not hold the daemon owner or shared hot path.
- Over-buffering terminal output can hide backpressure and grow memory; bounded queues need explicit drop policy and counters.
- Dropping control frames is riskier than dropping terminal frames. Control delivery should prefer bounded lag/error reporting over silent loss.
- Diagnostics can leak local paths, commands, session IDs, plugin payloads, or terminal data. Use bounded kinds/counters and scrubbed identifiers only.
- Public DTO additions can break generated TypeScript or older clients if fields are not optional/defaulted.
- Tests involving slow consumers can be flaky if they depend only on sleeps. Prefer deterministic blocked-reader fixtures, bounded timeouts, and explicit success events from a second client.
- Replacing unbounded queues too broadly could create deadlocks during daemon shutdown/detach cleanup.

## Acceptance checks/tests

- Add a real daemon protocol test where client A attaches and stops draining or blocks response consumption, client B attaches/drains/sends input, and client B still observes terminal output/control responses within a bounded timeout.
- Add a test where a slow/overloaded plugin or capability consumer reaches queue pressure and an unrelated session terminal/control path still drains and responds.
- Add a test that terminal backpressure/drop/lag diagnostics are structured, observable via existing daemon response/status paths, and contain no raw terminal payloads, local home paths, or plugin payload data.
- Add a test that plugin/capability queue pressure diagnostics are per-plugin or otherwise isolated and do not report another plugin's private payload.
- If public DTOs change, update generated TypeScript and tests that assert serde optionality/default behavior.
- Run targeted tests first, likely `./test.sh hub_daemon_lifecycle_test::...` and `./test.sh hub_capability_runtime_test::...` using test-name filters, then run full `./test.sh`.
- Verification must include a production entry-path statement: `botster_hub_client` request/connection/stream helpers -> `src/daemon_transport.rs` connection handling -> `HubClientApi` -> `HubRuntime`/core `SessionIo`/`ClientWorker`, with diagnostics returned through public daemon DTOs.

## Vault gaps worth capturing

- Capture a note if implementation establishes a durable rule for daemon adapter queue lanes, for example "daemon client egress queues classify terminal and control pressure separately".
- Capture a note if current `std::sync::mpsc` usage has a specific safe/unsafe boundary that future hub transport work should remember.
- Capture a note if plugin capability runtime pressure must be summarized differently from terminal/client pressure.
- No convention conflict found. The plan follows Botster architecture notes by keeping terminal data in `SessionIo`/`ClientWorker`, using hub-owned public daemon diagnostics, avoiding core/web edits, and preserving `./test.sh` as the verification harness.
