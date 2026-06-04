# Prove Hub Restart Reconnects Without Losing Sessions

## Context Loaded

- Project Pipelines current context for run `run_1780607155_510527`, step `botster_plan`, gate `botster_plan_gate`, ticket `ticket_1780532740_391550`.
- Ticket dependencies are closed: `Connect hub daemon to core daemon session runtime` and `Prove core daemon restart adopts live session workers`.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Botster architecture constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Ticket-specific constraints: [[botster hub daemon startup requires explicit data dir]], [[coredaemon embedding without worker path creates in process sessions]], [[adoption restart evidence must come from real protocol primitives not defaults]], [[sessionioworker is the production read path for session pty output]], [[botster terminal egress is session backed only]], and [[test script required for rust tests not cargo test]].
- Artifact discipline: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[pipeline artifacts should cite vault notes by wikilink not home path]], [[pipeline artifacts should use path neutral worktree references]], and [[botster review and verify must scan all committed artifacts for pii]].
- Repo context: `README.md` currently documents that live sessions are runtime-only and do not survive separate CLI invocations. `src/runtime.rs` and `src/daemon.rs` currently build `HubRuntime` over in-process `DefaultBotsterEngine`, not a `CoreDaemon` with external session workers. `Cargo.lock` currently resolves `botster-core` at `6ae1c60...`; remote `botster-core` main was observed at `f03e82e...` during planning.
- Project Pipelines checklist evidence: `project_pipelines_create_vault_checklist` timed out in the plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], checklist evidence is preserved in this plan and gate evidence instead of blocking Plan.

## Scope

- Add a daemon-backed hub runtime path that connects `botster-hub` to the production core daemon/session-worker runtime rather than proving restart through in-process `DefaultBotsterEngine`.
- Configure core daemon embedding with the `botster-session-worker` executable path. Absence of `.with_worker_path(...)` must be treated as a failed durability proof.
- Add hub-owned startup/restart reconciliation around the core daemon registry:
  - On hub start, read durable hub state and query the live core daemon registry.
  - Preserve live worker-backed sessions known to core even if the previous hub process exited.
  - Reconcile stale hub session records that no longer exist in core deterministically.
  - Reconcile core-live sessions missing from hub state deterministically, with sanitized recovered metadata.
- Add a focused integration proof that starts hub plus core daemon plus session worker, spawns a long-running session, stops only the hub-owned process/object, starts a new hub instance against the same core daemon/data root, lists/attaches/drains output from the same session, sends input when the session command accepts it, then shuts down cleanly.
- Update README and `docs/adr/hub-as-host-profile-over-core.md` to replace the current scaffold limitation that separate invocations cannot see live sessions and to document the deterministic hub/core session reconciliation contract.
- Keep the public proof path explicit-data-dir based: no HOME/XDG fallback and no path-bearing output.

## Non-Scope

- No new browser, TUI, Rails, cloud, WebRTC, ActionCable, marketplace, OAuth, or provider process feature.
- No hub-owned PTY byte loop, terminal fallback streamer, or snapshot parser. Terminal egress stays in core/session/client worker paths.
- No broad rewrite of package admission, capability runtimes, plugin lifecycle, or existing local client API semantics beyond adapting them to the daemon-backed runtime.
- No speculative multi-daemon configurability. Add only the configuration needed to resolve the session worker executable and share the explicit data root.
- No test-only shortcut that fabricates a session registry, fake attach result, or optimistic restart evidence.

## Assumptions And Unknowns

- Assumption: the closed dependency tickets added the necessary core daemon/session-worker APIs on current `botster-core` main. Implementation should refresh/add dependencies deliberately and inspect the actual exported API before coding.
- Assumption: `botster-core-daemon` is the correct crate/API for the hub to embed or connect to, and `botster-session-worker` is the durable session owner for this proof.
- Assumption: the hub restart in this ticket means hub-owned policy/state/runtime object or process restart while the core daemon and session worker continue running.
- Assumption: core daemon registry is authoritative for live worker-backed sessions; hub durable state is authoritative for hub policy and presentation metadata.
- Unknown: the exact core API names for listing/adopting live sessions, attaching to an existing worker-backed session, and accessing protocol evidence. Implementer must use real current core API names after lock refresh.
- Unknown: whether the hub should maintain an explicit durable session map today or derive recovered session presentation from core metadata at start. Prefer the smallest deterministic implementation that can document stale/missing behavior.
- Unknown: whether a binary-level restart proof is practical in this scaffold. If core daemon embedding can be kept in-process while session workers remain external, an integration test that drops/recreates the hub daemon object is acceptable only if it proves the same production daemon-backed runtime path.
- Implementation decision required: after refreshing and inspecting the current core daemon API, replace the standing in-process `DefaultBotsterEngine` session path with the daemon-backed path for hub restart semantics. Do not leave a permanent parallel runtime mode. If a short transitional adapter is unavoidable, Implement must document why both paths temporarily exist and include a concrete follow-up/removal decision.

## Affected Surfaces And Files

- `Cargo.toml` and `Cargo.lock`: add or refresh daemon/session-worker dependencies only as required by current core APIs.
- `src/runtime.rs`: replace the session-facing in-process `DefaultBotsterEngine` path with a daemon-backed runtime path for hub daemon/session continuity. Keep explicit hub facade methods and avoid exposing raw core routers. A temporary adapter is acceptable only as implementation scaffolding with a documented removal decision.
- `src/daemon.rs`: wire `HubDaemon::start` to initialize/reconnect through the core daemon-backed runtime, restore hub state, and run deterministic reconciliation.
- `src/client_api.rs`: ensure list, attach, input, drain, inspect/snapshot, and shutdown requests route through the daemon-backed runtime path without changing admission semantics.
- `src/main.rs`: keep CLI entrypoints thin and facade-backed. Add a restart-proof smoke command only if it is needed for user-path evidence and can stay deterministic.
- `src/config.rs`: add worker path resolution/config only if the daemon-backed runtime needs it. Keep explicit data-dir as the durable startup input.
- `src/persistence.rs`: persist only hub-owned state needed for reconciliation. Do not store PII, raw local paths, or core-private protocol state.
- `README.md` and `docs/adr/hub-as-host-profile-over-core.md`: document daemon-backed restart behavior and hub/core disagreement rules.
- `tests/hub_daemon_lifecycle_test.rs` or a new Unix integration test file: add the restart proof. Existing in-process tests should remain as lower-level facade coverage or be updated if the runtime path is fully replaced.

## Planned Behavior

- Startup:
  - Build explicit `HubConfig` from `--data-dir` or test-provided options.
  - Resolve the core daemon data root under the hub data directory.
  - Resolve `botster-session-worker` explicitly and pass it into core daemon configuration.
  - Start or connect to the core daemon runtime, then query live sessions before reporting hub ready.
- Reconciliation:
  - If hub state has a session and core registry confirms it live, mark it available/reconnected.
  - If hub state has a session and core registry says missing/dead, mark it stale or remove it according to the smallest existing hub-state shape; document the deterministic choice.
  - If core registry has a live session missing from hub state, surface it as recovered/adopted with sanitized metadata from core, not invented hub metadata.
  - Never treat path existence, registry metadata alone, or default boolean fields as proof of a recoverable session. Evidence must come from actual core daemon/session-worker APIs.
- Restart proof:
  - Spawn a long-running shell loop through `HubDaemon -> HubClientApi -> daemon-backed runtime`.
  - Attach and drain a readiness marker.
  - Drop/stop only the hub lifecycle while keeping the core daemon and session worker alive.
  - Start a new hub lifecycle using the same explicit data root.
  - List the same session id, attach to it, drain continuing output or a retained snapshot, send input if the command is interactive, observe echoed output, and shut down through the same client API.
- Disagreement proof:
  - Seed or produce a hub-known session whose core daemon record is missing or dead. Restart/reconcile and assert the documented stale outcome.
  - Seed or produce a core-live worker-backed session missing from hub state. Restart/reconcile and assert it surfaces as recovered/adopted with sanitized metadata from real core/session-worker evidence.
  - Keep both disagreement checks on the daemon-backed production path. Registry-shaped fixtures may set up the scenario, but pass/fail evidence must come from current core daemon/session-worker APIs.

## Risks

- False-positive proof: continuing to use `DefaultBotsterEngine::new()` or in-process PTYs would pass existing tests but fail the ticket. Review should reject any restart proof lacking the core daemon plus explicit worker path.
- Dependency drift: the current lockfile is behind remote `botster-core` main. Implementer must inspect refreshed APIs and avoid inventing wrappers around unavailable methods.
- Underwired implementation: adding a daemon-backed type without routing `HubDaemon`, `HubClientApi`, or a documented CLI/user path through it would not satisfy acceptance.
- Data-plane ownership drift: hub code must not relay PTY bytes itself to bridge restart behavior.
- Stale/missing reconciliation ambiguity: if hub and core disagree, the behavior must be deterministic and documented, not incidental to hash map iteration or test timing.
- Runtime-mode fork: keeping both in-process and daemon-backed session runtime paths after this ticket would preserve the ambiguity the ticket is meant to remove. Implement must resolve the fork or document a temporary transition with a removal decision.
- Process leaks/flakes: integration tests must always shut down core daemon/session worker processes, even after failed assertions.
- PII leakage: plan/docs/test fixtures/log output must not include local absolute paths, user names, or sensitive command data.

## Acceptance Checks And Tests

- `./test.sh --test <restart integration test name>` proving:
  - explicit data-dir startup,
  - core daemon configured with the session-worker executable,
  - long-running session spawn through hub/client API,
  - hub-only restart,
  - relist/re-attach to the same live session id,
  - output drain after restart,
  - input after restart if applicable,
  - clean shutdown of session, core daemon, and hub lifecycle.
- `./test.sh --test <reconciliation integration test name>` proving both deterministic disagreement branches:
  - hub-known session missing/dead in core becomes the documented stale outcome,
  - core-live worker-backed session absent from hub state becomes the documented recovered/adopted outcome with sanitized metadata,
  - both assertions use real current core daemon/session-worker APIs for evidence rather than defaulted positive fields.
- `./test.sh --test hub_daemon_lifecycle_test` remains passing or is updated to cover daemon-backed lifecycle.
- `./test.sh --test hub_client_api_test` remains passing or is updated so client API requests exercise the daemon-backed runtime where appropriate.
- Full `./test.sh` before Review unless runtime cost is prohibitive; if scoped, explain why and list the exact skipped surface.
- `cargo clippy --all-targets --all-features -- -D warnings` for changed Rust code. If it fails, capture raw diagnostics and attribute each failure to touched files versus pre-existing baseline before assigning ticket blame.
- Manual/user-path smoke, if implemented: an explicit-data-dir command that demonstrates restart/reconnect without printing local paths.
- Documentation check: README and `docs/adr/hub-as-host-profile-over-core.md` document daemon-backed restart behavior and the deterministic stale/missing reconciliation contract; README no longer claims live sessions cannot survive separate hub invocations once the restart path ships.
- PII scan over committed diff and generated test output snippets: no local absolute home paths, email addresses, or secret-bearing metadata.

## Pipeline Gates And Artifacts

- This plan document is the repo-visible Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Gate evidence should point to this file and include the checklist fallback note because checklist creation timed out.
- Plan Review should verify that the plan does not waive the ticket's "new daemon-backed production path" requirement.
- Implement should report the exact refreshed core dependency revision, the production entry point that uses daemon-backed reconnect behavior, the deliberate runtime-mode decision, and the exact docs section where the reconciliation contract landed.
- Verify should re-run the restart proof against the live worktree, not trust an Implement-stage status.

## Vault Gaps Worth Capturing

- Capture a durable note if `botster-hub` adopts `CoreDaemon` as its standing runtime facade, especially the exact worker-path resolution and restart reconciliation contract.
- Capture the deterministic hub/core session disagreement policy once implementation settles it.
- Capture any `botster-core-daemon` API gotchas discovered while wiring current main into `botster-hub`.
- Capture recurring Project Pipelines checklist worker timeouts if they continue beyond this Plan step; this run already needed the documented artifact/gate fallback.
