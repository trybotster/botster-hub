# Connect Hub Daemon To Core Daemon Session Runtime

Ticket: `ticket_1780532739_854865`

## Context Loaded

- Project Pipelines context loaded for run `run_1780595768_169284`, step `botster_plan`, gate `botster_plan_gate`.
- Ticket: `Connect hub daemon to core daemon session runtime`.
- Dependencies are closed:
  - `Add core daemon supervisor and persistent session registry`
  - `Prove core daemon restart adopts live session workers`
- No prior artifacts, reviews, findings, open questions, or question answers were present.
- Required playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]].
- Required Botster overlay notes loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Additional constraining notes loaded: [[botster hub is a first party host profile over core]], [[botster local client api lives over hubruntime not raw core routers]], plus identity/goals context.
- Repo context inspected:
  - `Cargo.lock` pins `botster-core` at `6ae1c601ef6d9963a0dcd460257a24f5d3e0775c`.
  - `src/daemon.rs` starts an in-process `HubRuntime` and restores hub package policy, but does not discover or connect to a separate core daemon.
  - `src/runtime.rs` wraps `DefaultBotsterEngine` directly for spawn/list/attach/input/resize/drain/snapshot/shutdown.
  - `src/client_api.rs` routes local client requests through `HubRuntime`, still in-process.
  - `tests/hub_runtime_test.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `tests/hub_local_dogfood_test.rs` prove the current embedded runtime path.
  - `README.md` currently documents the in-process scaffold and explicitly says cross-process live session continuity is not supported.
- Locked core dependency context inspected from Cargo's git checkout:
  - `DefaultBotsterEngine` exposes session operations over `ManagedSessionRuntime<LocalProcessRuntime>`.
  - Core has notification inbox primitives on the generic `BotsterEngine` path, while docs state `DefaultBotsterEngine` does not expose notification inbox methods at this revision.
  - The currently locked checkout did not show an obvious `CoreDaemon` public type by name, so implementation must verify whether the closed prerequisite APIs require a dependency refresh before coding.
- Project Pipelines checklist discipline:
  - `project_pipelines_checklist_instructions` loaded.
  - `project_pipelines_create_vault_checklist` failed with a plugin worker timeout. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and should be copied into gate evidence.

## Scope

In scope:

- Move the hub production local session path from embedded `HubRuntime -> DefaultBotsterEngine` ownership to the durable core daemon/session-worker model supplied by `botster-core`.
- Add or adapt a hub-owned typed core daemon client/control boundary that starts, discovers, and connects to the core daemon through the blessed typed API from the closed core prerequisite tickets. Do not shell out to, parse, or screen-scrape a core CLI.
- Route production hub session commands through core daemon primitives for:
  - spawn
  - list
  - attach
  - drain/output
  - input
  - resize
  - guarded session notification/write
  - shutdown
- Preserve hub policy ownership for package/provider admission, capability grants, auth/config decisions, data-dir selection, package lifecycle state, and which plugins/providers may request guarded session notification writes.
- Treat plugins as consumers of hub-granted guarded session notification/write capability. The mechanism belongs to core; hub grants or denies access.
- Keep semantic hints from hub/plugin policy as metadata only, such as "waiting for human answer". Core owns terminal-state observation, readiness evidence, queue/defer mechanics, and delivery transitions.
- Rename or isolate the current embedded local runtime path as dev/test-only if it remains useful for scaffold tests.
- Update docs so they describe hub as a host profile/control plane over core daemon/session workers and distinguish typed API usage from core daemon CLI usage.
- Add tests proving the production/user path changed, not only that new types compile.

Non-scope:

- No Rails, ActionCable, WebRTC, browser SPA, TUI, cloud/provider process implementation, OAuth/device-code flow, marketplace, or package-index work.
- No new core daemon mechanism in `botster-hub`; if the required core API is absent from the dependency, refresh the dependency or ask a human before inventing a duplicate.
- No shelling out to a core daemon CLI or parsing CLI output.
- No hub-owned PTY handles, terminal output readers, readiness classifiers, guarded-write queues, or delivery state machines on the production path.
- No broad refactor of package policy, plugin lifecycle, capability runtimes, persistence, or docs beyond what is needed to route sessions through the core daemon boundary.
- No PII in fixtures, state, logs, docs, or tests.

Botster layers touched:

- Rust hub crate: primary.
- Rust core daemon/session-worker client API: consumed from `botster-core`; dependency may need refresh to the closed prerequisite revision.
- Hub local client API: adapted so session verbs delegate through the daemon-backed production path.
- Hub capability/package policy: used for grant decisions, not widened into core mechanisms.
- Docs and integration tests.
- Plugin, Lua, MCP, TUI, React SPA, Rails relay: not implemented.

## Assumptions And Unknowns

Assumptions:

- "Core daemon" means the durable daemon/session-worker API delivered by the two closed dependencies, not the current embedded `DefaultBotsterEngine` facade.
- The first implementation step should verify whether `Cargo.lock` already contains those APIs. If not, updating the `botster-core` git dependency to the merged prerequisite revision is in scope because the ticket depends on those closed APIs.
- The hub can still own `HubDaemon` as startup and policy/control-plane composition, but live production session handles must be owned by core daemon/session workers.
- Tests may remain Unix-only where they spawn local PTYs.
- The smallest acceptable implementation is a daemon-backed local client/control path plus docs/tests; it does not need browser/TUI/socket UI adapters.
- Any retained embedded runtime path must be explicitly named `dev`, `test`, or equivalent and excluded from production local session commands.

Unknowns:

- Exact public names and signatures for the core daemon typed API, persistent registry, guarded notification/write primitive, and adoption/restart evidence in the prerequisite `botster-core` revision.
- Whether `botster-core` exposes notifications through the same default local daemon facade or through a separate generic engine/control API. Implementation must not bridge this by adding hub-owned notification delivery semantics.
- Whether `HubClientApi` should keep its public request enum unchanged and swap the backend, or split runtime-backed and daemon-backed handlers. Prefer preserving request vocabulary if the daemon API can satisfy it cleanly.
- Whether the current `HubDaemon::start` should start the core daemon when absent or only discover/connect to an already-running daemon. Ticket wording allows start/discover/connect; implementer should choose the smallest deterministic lifecycle supported by core and test it.
- Whether a real cross-process daemon process is available in the dependency. If the closed core dependency still exposes only in-process types, ask a human before treating that as sufficient, because that may waive the ticket's "core daemon" wording.

No human question blocks the plan, but implementation must ask one before proceeding if the refreshed core dependency still lacks a typed core daemon/session-worker control API or a guarded session notification/write primitive.

## Affected Surfaces / Files

Expected production changes:

- `Cargo.lock`
  - Refresh `botster-core` to the closed prerequisite revision if current `6ae1c60` lacks the daemon/session-worker APIs.
- `src/runtime.rs`
  - Replace production `DefaultBotsterEngine` ownership with a daemon-backed adapter/client or split the embedded runtime into a clearly dev/test-only type.
  - Keep hub facade methods explicit; do not expose raw core command routers.
- `src/daemon.rs`
  - Start/discover/connect to the core daemon from explicit hub config/data-dir.
  - Keep package/provider policy restoration and scrubbed status in hub.
  - Report whether the core daemon connection is live without leaking local paths.
- `src/client_api.rs`
  - Route session request variants through the core daemon client/control API.
  - Add guarded session notification/write request/response types only if the core primitive is available and hub policy can grant/deny it.
  - Preserve explicit admission failures for denied clients/plugins/providers.
- `src/capabilities.rs`, `src/profile.rs`, `src/packages.rs`
  - Add only the narrow grant/admission wiring needed to allow/deny plugin/provider guarded notification writes.
  - Do not define a parallel capability primitive if core already owns it.
- `src/main.rs`
  - Keep CLI thin and route dogfood session commands through hub's typed local API, which must now use the core daemon path.
  - Remove or relabel output that implies separate CLI invocations cannot share live sessions if the daemon-backed path now supports continuity.
- `src/lib.rs`
  - Export new narrow daemon/client types and update architecture audit strings only as needed.
- `README.md` and `docs/adr/hub-as-host-profile-over-core.md`
  - Update the production architecture description: hub is host profile/control plane over core daemon/session workers; typed API is required; core daemon CLI parsing is forbidden.
- New or updated plan/doc artifact:
  - This file should remain the Plan artifact.

Expected tests:

- `tests/hub_core_daemon_session_runtime_test.rs` or focused additions to existing hub daemon/client tests.
- Update `tests/hub_local_dogfood_test.rs` to prove the dogfood path now survives the daemon-backed production boundary or explicitly document any remaining scaffold-only pieces.
- Existing embedded-runtime tests should be renamed or scoped if they are retained for dev/test-only behavior.

Likely unchanged unless compile fixes require it:

- `src/auth.rs`
- `src/persistence.rs`
- `src/lifecycle.rs`
- plugin fixture files under `examples/`

## Implementation Shape

1. Verify the prerequisite core API in the live dependency.
   - Inspect exported `botster-core` types and docs for core daemon startup/discovery/client/control, persistent session registry, session adoption evidence, and guarded notification/write.
   - If absent in current lockfile, refresh the dependency to a revision containing the closed prerequisites.
   - If still absent after refresh, ask a human question rather than implementing a hub-owned substitute.

2. Introduce a hub-owned daemon connection boundary.
   - Prefer a small type such as `HubCoreDaemonClient` or `HubSessionRuntimeClient` that wraps the blessed core API.
   - It should be constructed from explicit hub config/data-dir and package/admission context.
   - It should be the production backend used by `HubDaemon` and `HubClientApi` session operations.

3. Preserve hub policy before crossing into core.
   - For ordinary local session operations, authenticate/admit the local client through hub policy, then call core daemon primitives.
   - For guarded session notification/write, require a hub package/provider grant decision before calling core.
   - Pass semantic hints as metadata only; core owns readiness/gating/queueing/delivery state.

4. Demote the embedded runtime path.
   - If current tests still need embedded `DefaultBotsterEngine`, move it behind a dev/test-only name or feature and make docs clear it is not the production local session path.
   - Do not keep parallel production routing branches.

5. Update tests and docs.
   - Add integration tests that prove production entry points call the daemon-backed client and that session operations work through it.
   - Add allow/deny tests for plugin/provider guarded notification writes.
   - Update README/ADR to explain the typed API boundary and the core CLI non-use rule.

## Risks

- Stale dependency risk: the current lockfile does not obviously expose core daemon types by name. Planning around stale APIs would recreate closed core work in the hub.
- Boundary collapse: keeping `HubRuntime` as a production owner of `DefaultBotsterEngine` would fail the ticket even if tests still pass.
- Dual-path ambiguity: retaining both embedded and daemon-backed production paths would obscure ownership. Prefer a cold replacement; any retained embedded path must be dev/test-only by name and docs.
- CLI parsing shortcut: shelling out to a core CLI would satisfy a smoke demo while violating the typed API requirement.
- Guarded write overreach: hub may decide allow/deny and hints, but must not own readiness evidence, queue/defer mechanics, delivery state transitions, or terminal observation.
- Data-plane duplication: hub must not reintroduce PTY readers or terminal byte fanout.
- PII leakage: daemon status, fixture state, and logs must avoid local paths, usernames, hostnames, tokens, command transcripts with sensitive data, and environment dumps.
- Test false positives: tests that instantiate only `DefaultBotsterEngine` or compile protocol structs would not prove the production path changed.

## Acceptance Checks / Tests

Required verification after implementation:

- `cargo fmt`
- Focused daemon/session tests, for example:
  - `cargo test --test hub_core_daemon_session_runtime_test`
  - `cargo test --test hub_client_api_test`
  - `cargo test --test hub_local_dogfood_test`
- Full crate regression:
  - `cargo test`
- Strict lint if the repo policy remains warning-clean:
  - `cargo clippy --all-targets --all-features -- -D warnings`
- Static scans:
  - `rg -n "DefaultBotsterEngine|HubRuntime::new|HubRuntime::load" src tests README.md docs`
    - Production session path references must be removed or clearly dev/test-only.
  - `rg -n "Command::new|core daemon|botster-core" src tests`
    - No core daemon CLI shell-out/parsing path should appear for session routing.
  - `rg -n "BoundaryJson|serde_json::Value" src/client_api.rs src/runtime.rs src/daemon.rs`
    - Guarded notification/write stable controls must remain typed.
  - Run the standard changed-file PII scan for local path, environment, credential, and identity markers.
    - No introduced PII/local-path leaks.

Acceptance assertions:

- Hub starts/discovers/connects to the core daemon through the typed local client/control API.
- Hub session spawn/list/attach/drain/input/resize/shutdown all route through the core daemon/session-worker path.
- Tests prove the actual runtime path through `HubDaemon` or `HubClientApi`, not only direct core API use.
- Hub policy can allow a plugin/provider guarded session notification write and the write is delivered by core-owned readiness/gating/queueing/delivery mechanics.
- Hub policy can deny the same guarded write before core delivery is requested.
- Semantic hints are passed as metadata only; tests or code structure show core owns delivery state transitions.
- Docs explicitly state hub is a host profile/control plane over core daemon/session workers and that core daemon CLI parsing is not allowed.
- Existing dogfood smoke behavior keeps working through the daemon-backed path or is intentionally replaced with a documented daemon-backed proof.

## Pipeline Gates And Artifacts

- Plan gate artifact: this file.
- Checklist evidence fallback:
  - Vault/project notes read are listed in Context Loaded.
  - Convention conflicts: none, assuming implementation uses the core daemon typed API and does not keep embedded production PTY ownership.
  - Verification commands are listed in Acceptance Checks / Tests.
  - Durable capture: no pre-implementation vault capture required; capture after implementation if core daemon/hub guarded-write boundaries settle reusable rules.
- Plan Review should reject:
  - any plan that implements a hub-owned replacement for missing core daemon primitives without a human answer
  - CLI shell-out/parsing as the core daemon connection
  - production `DefaultBotsterEngine` session ownership
  - guarded notification/write readiness or delivery state in hub
  - broad provider/cloud/Rails/WebRTC/TUI/browser implementation

## Vault Gaps Worth Capturing

- Capture after implementation if the hub/core boundary establishes a durable rule for "hub daemon starts/discovers/connects to core daemon" naming and lifecycle ownership.
- Capture after implementation if guarded session notification/write grants become a reusable capability convention for plugins/providers.
- Capture if the dependency refresh reveals a standing Project Pipelines gotcha: hub runs can be cut from a lockfile that predates closed core dependency APIs.
- Capture if Project Pipelines checklist creation continues timing out for plan agents; this run hit the documented checklist worker timeout path and used gate/artifact fallback.
- No new capture is needed for existing hub-as-host-profile, data-plane ownership, local client over hub facade, explicit target/worktree orchestration, or repo-visible plan artifact rules.
