# Expose stable local client API over hub commands and events

## Context Loaded

- Pipeline context: ticket `ticket_1780508732_387130`, run `run_1780524545_186502`, step `botster_plan`, one closed prerequisite for explicit hub daemon startup lifecycle, no prior artifacts/findings/questions/answers.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster vault overlays: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]].
- Repo context inspected: `Cargo.toml`, `test.sh`, `README.md`, `docs/adr/hub-as-host-profile-over-core.md`, prior `docs/plans/*`, `src/lib.rs`, `src/runtime.rs`, `src/packages.rs`, `src/auth.rs`, `src/main.rs`, `tests/hub_runtime_test.rs`, and locked `botster-core` public contracts at the Cargo.lock revision.
- Current repo shape: `botster-hub` is a Rust host-profile crate. `HubRuntime` already wraps `DefaultBotsterEngine` with explicit methods for session list/spawn/attach/input/read screen/snapshot/drain/pressure/shutdown and intentionally hides the generic core command router. `PackageAdmissionPolicy`/`PackageRegistry` already provide in-memory package/provider admission policy over `botster-core` manifest/capability contracts.
- Checklist evidence: attempted to create the standard Project Pipelines vault checklist for the run, but the plugin worker timed out. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan carries the vault notes read, conflict result, verification strategy, and capture decision in the repo artifact and gate evidence.

## Scope

- Add a stable, typed, transport-neutral local client API boundary in `botster-hub`.
- Route client requests through hub-owned admission/policy and the existing `HubRuntime`/`PackageAdmissionPolicy` facades, not through raw `DefaultEngineCommand` or direct core internals.
- Cover the ticket's request surface with explicit hub request variants and response/event types:
  - status
  - session list
  - spawn
  - attach
  - detach
  - input
  - resize
  - screen/snapshot where core supports them
  - package/provider list/query path
  - plugin lifecycle status
- Add an in-process local client harness first. This is enough to prove the transport-neutral protocol and leaves Unix socket framing as a later adapter over the same contract.
- Keep local auth/admission explicit with a localhost/operator-style grant shape, even if the first implementation grants the test/operator harness by default.
- Add focused tests that exercise the production request path through `HubRuntime` and package/lifecycle facades, including session events from core output.
- Update public docs only where needed to name the client API boundary and explain why it is scaffolded as in-process transport-neutral protocol.

## Non-Scope

- No TCP/WebRTC/ActionCable/Rails/React/TUI client implementation.
- No full Unix socket daemon if the in-process harness satisfies the acceptance path. A socket adapter can be layered later over the same request/event protocol.
- No marketplace, package fetching, external provider implementation, OAuth/device-code auth, cloud federation, secret storage, or package persistence.
- No raw filesystem path exposure in request/response/event payloads. Spawn requests should use admitted/session-default context already resolved by hub config, not arbitrary client-supplied host paths.
- No generic pass-through command that accepts `DefaultEngineCommand`, raw `TransportIngress`, arbitrary `BoundaryJson`, or unconstrained file paths.
- No broad refactor of `HubRuntime`, package policy, plugin lifecycle, config, or prior docs beyond edits necessary to expose and document the client boundary.

## Assumptions And Unknowns

- Assumption: the ticket allows an in-process harness instead of a concrete Unix socket transport because it asks for a local socket or transport-neutral request/event protocol.
- Assumption: "local client API" belongs in the Rust hub crate as a host-profile client contract, not in Lua plugins, Rails, browser SPA, or TUI.
- Assumption: provider/package query path can initially read installed/admitted package records from the in-memory `PackageAdmissionPolicy`/`PackageRegistry`; it should not invent marketplace or provider runtime discovery.
- Assumption: plugin lifecycle status can be a narrow hub-owned read model over `HubPluginLifecycle`/known loaded packages if the existing lifecycle facade exposes enough state. If not, add the smallest lifecycle status accessor needed and test it.
- Unknown: whether `HubPluginLifecycle` currently has a public status/list accessor. Implementer must inspect `src/lifecycle.rs`; add a narrow accessor only if needed for plugin lifecycle status acceptance.
- Unknown: whether screen/snapshot responses should include opaque bytes from `TransportEgress::Snapshot`/screen frames or only acknowledge request acceptance. Prefer returning typed events produced by `HubRuntime` output so the user path is proven without inventing snapshot semantics.
- Unknown: whether a real local socket transport is required by downstream clients immediately. If Plan Review reads the ticket as requiring a concrete socket, ask a human question before implementation rather than silently scoping to in-process.

## Affected Surfaces And Files

- `src/client_api.rs` or equivalent new module:
  - typed request/response/event protocol
  - client identity/admission context
  - in-process harness/session state
  - translation from `HubRuntimeOutput` and package/lifecycle reads into local client events
- `src/lib.rs`:
  - public exports for the stable client API
  - architecture facade audit entry for the new boundary
- `src/runtime.rs`:
  - likely add `detach_client` and `resize` wrappers because core supports them and the ticket requires them.
  - avoid exposing the generic core command router.
- `src/auth.rs`:
  - likely add a local client admission hook or policy enum/value so auth/admission remains explicit.
- `src/lifecycle.rs`:
  - possibly add read-only plugin lifecycle status accessor if none exists.
- `src/packages.rs`:
  - likely add read-only package/provider list/query helpers if the existing registry accessors are insufficient or too low-level.
- `tests/hub_client_api_test.rs`:
  - exercise status/list/spawn/attach/input/session events/package-provider query through the in-process local client API.
  - include detach/resize coverage if added to the request protocol.
- `tests/hub_runtime_test.rs`:
  - adjust only if `detach_client`/`resize` wrappers deserve direct facade evidence beside the client API test.
- `README.md` and `docs/adr/hub-as-host-profile-over-core.md`:
  - update only if needed to name the stable local client API boundary and clarify scaffold limits.

## Implementation Shape

1. Define a hub-owned client protocol.
   - `HubClientRequest` should be an enum with narrow variants, each carrying typed ids, request ids, and bounded payloads.
   - `HubClientResponse` should separate synchronous request success/failure from emitted events.
   - `HubClientEvent` should be the stable local event stream over session lifecycle, attach state, terminal output/snapshot/screen availability, process exit, pressure/lag/failure observations, package/provider state, and plugin lifecycle status.
   - Errors should be typed and path-neutral.

2. Add explicit local admission.
   - Introduce a small `LocalClientAdmission`/`HubClientAdmission` value or method that maps a local operator/client identity to allowed request categories.
   - Tests may use an admitted local operator identity, but the API should make denial possible and covered by at least one test.

3. Wire a production path through facades.
   - `HubClientHarness` or `HubClientApi` should own or borrow `HubRuntime`, `PackageAdmissionPolicy`, and plugin lifecycle/package status surfaces.
   - Request handlers call explicit `HubRuntime` methods such as `list_sessions`, `spawn_session`, `attach_client`, `write_bytes`, `detach_client`, `resize`, `read_screen`, `capture_snapshot`, and package/lifecycle status helpers.
   - Convert `HubRuntimeOutput` into `HubClientEvent` without remapping terminal bytes into fake session-worker events.

4. Keep package/provider query path scoped.
   - Return installed package/provider records, enabled state, classification, requested capabilities, and admission status/audit context available from existing policy.
   - Do not fetch package metadata, resolve local package paths, or expose provenance paths beyond sanitized/package-level metadata already safe for operator review.

5. Document the boundary.
   - README/ADR should state that the local API is transport-neutral and can back CLI/TUI/local bridge/socket adapters, while concrete network/browser transports remain outside this scaffold.

## Risks

- Boundary collapse: exposing `DefaultEngineCommand`, `TransportIngress`, raw `BoundaryJson`, or direct core engine internals would bypass hub policy. Tests should assert the public client API uses hub types and explicit methods.
- Data-plane ownership drift: terminal bytes and snapshots must still flow from `DefaultBotsterEngine`/SessionIo/ClientWorker-backed output, not a new hub-side byte stream.
- Scope creep: a real socket server, marketplace, provider runtime, external auth, or browser bridge would exceed the ticket.
- Path/PII leakage: spawn, package provenance, errors, and docs must avoid absolute local paths and user-identifying data.
- Partial acceptance: adding protocol structs without the in-process harness would not prove the actual runtime path changed.
- Plugin lifecycle ambiguity: if lifecycle status is not currently readable, the added accessor must stay read-only and not turn into lifecycle management policy.

## Acceptance Checks And Tests

- `./test.sh hub_client_api` or equivalent focused test filter:
  - status request returns hub/profile/runtime status through the client API.
  - session list initially returns empty state through the client API.
  - spawn request creates a real local session through `HubRuntime`.
  - attach request subscribes a client and emits typed attach/session events.
  - input request sends bytes to the session and emitted terminal output contains the expected response.
  - detach request unsubscribes through the explicit hub facade if implemented.
  - resize request reaches the explicit hub facade if implemented.
  - screen/snapshot request produces typed success/events or a typed unsupported/error response from the real core path.
  - package/provider query returns package/provider records through `PackageAdmissionPolicy`/`PackageRegistry`, not static duplicate capability vocabulary.
  - plugin lifecycle status returns loaded/unloaded/known state through the lifecycle facade.
  - denied/unadmitted client request fails with a typed admission error.
- `./test.sh` for full crate regression.
- Static scans:
  - `rg -n "DefaultEngineCommand|execute_command" src tests` should show no client API pass-through usage; existing architecture audit references are acceptable.
  - Run a local-path/PII scrub over `README.md`, `docs`, `src`, and `tests`; it should show no committed local path or identity leaks introduced by the change.
  - `rg -n "BoundaryJson|TransportIngress" src/client_api.rs tests` should show no public client request escape hatch unless the implementation explicitly documents a safe internal conversion.

## Production Entry Point Proof

Implementation evidence must identify the exact entry point clients use, for example `HubClientApi::handle_request` or `HubClientHarness::request`, and show that it invokes `HubRuntime`/package/lifecycle facades. Evidence that protocol types compile is not enough.

## Pipeline Gates And Artifacts

- Plan gate artifact: this document.
- Implement gate should attach:
  - changed production entry point
  - focused and full test commands
  - static scan outputs
  - explanation of any scaffold-only areas
- Review should reject:
  - raw core command/router exposure
  - arbitrary filesystem path payloads
  - protocol-only code without harness/runtime proof
  - speculative socket/provider/marketplace/auth/browser work

## Vault Gaps Worth Capturing

- Capture if implementation settles a durable rule for where stable hub-local client API contracts live relative to `HubRuntime`, CLI/TUI/socket adapters, and core transport contracts.
- Capture if a reusable rule emerges for local operator admission grants over hub commands.
- Capture if plugin lifecycle status needs a narrow read-only accessor convention distinct from lifecycle mutation methods.
- No new vault capture is needed for the existing core/hub/plugin/provider boundary, data-plane ownership, package/provider policy, or plan artifact discipline; existing notes cover those constraints.
