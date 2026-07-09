# Hub Single CoreDaemon Coordination Owner

## Context Loaded

- Pipeline context: ticket `ticket_1783552997_748095`, run `run_1783615936_382824`, step `botster_plan`, gate `botster_plan_gate`.
- Ticket intent: remove dual routed-envelope/notification coordination for hub-native plugins and MCP. CoreDaemon should be the single coordination bus, with HubRuntime acting only as a pass-through to daemon APIs.
- No prior artifacts, findings, dependencies, open questions, or answers were present when planning started.
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster/vault constraints loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[hub coordination envelopes need one sanctioned owner]], [[botster local client api lives over hubruntime not raw core routers]], [[botster plugin runtime uses supervisor plus per plugin workers]], [[botster core lua owns plugin framework primitives not product policy]], [[cold turkey migrations eliminate dual code paths and version suffixes]], [[coredaemon must expose terminal truth used by the production hub path]], [[coredaemon embedding without worker path creates in process sessions]], [[botster plugins manage notifications through scoped claims not delivery ownership]], [[notify session default readiness evidence structurally defers writes]], and [[plan steps need reviewable plan artifacts]].
- Plan Review returned changes required on prior versions. This revision addresses the follow-up correction: Lua and native already share HubRuntime's local router, so the discriminating proof must observe CoreDaemon's router directly. It also drops the unbuildable shutdown-gating test and scopes documentation grep away from this plan artifact.
- Checklist evidence: the first checklist create call returned a plugin-worker timeout, but the run checklist later appeared as `checklist_1783616125_698439` and has been updated with vault-note, convention-conflict, verification, and capture evidence.

## Scope

- Remove HubRuntime's separate in-memory `RoutedEnvelopeRouter` from the product coordination path.
- Route `HubRuntime::publish_routed_envelope`, `drain_routed_envelopes`, and `acknowledge_routed_envelope` through `CoreDaemon::{publish,drain,acknowledge}_routed_envelope`.
- Rework Lua `botster.coordination.*` helpers so plugin worker calls use a host-provided coordination bridge that delegates to the same `CoreDaemon` instance used by native daemon/MCP calls.
- Preserve caller/plugin endpoint identity for Lua-published envelopes; Project Pipelines should still publish from `plugin:<plugin_key>` and target plugin/session endpoints with the same payload shape.
- Record the notification audit result: hub has no parallel notification inbox. `GuardedNotificationWrite` and `NotifySession` route through `HubClientApi -> HubRuntime::guarded_write -> CoreDaemon::guarded_write`; readiness semantics are already CoreDaemon-owned and remain unchanged.
- Update docs/comments that currently overstate or obscure routing. `README.md:591-592` and `src/lib.rs:328-330` already claim CoreDaemon delegation before the code does it and should become true after implementation. `README.md:607-609` implies Project Pipelines composes a separate Lua-routed primitive and must be rewritten. `src/runtime.rs:216-220` justifies hub router ownership and must be deleted with the field.
- Add or update tests that prove native MCP and Lua/Project Pipelines coordination share the single CoreDaemon path and that routed-envelope queues remain process-memory only across daemon restart.

## Non-Scope

- Persisting routed envelopes across daemon restart.
- Inventing broader core APIs beyond the thin daemon methods already present.
- Changing guarded-write readiness semantics for `notify_session`; default readiness may still defer until a separate ticket adds observed terminal readiness.
- Adding a hub-local notification inbox, notification router, or notification delivery fallback.
- Reworking Project Pipelines workflow semantics, UI surfaces, persistence schema, agent spawning, worktree ownership, or notification-badge policy beyond what is required to keep its coordination proof on the single bus.
- Broad refactors of capability runtime, package registry, daemon transport, TUI, WebRTC, or browser clients.

## Assumptions And Unknowns

- `botster_core_daemon::CoreDaemon` in the locked dependency exposes routed-envelope methods named by README and `src/lib.rs`. Repo inspection found publish/drain/ack methods in the locked git checkout and confirmed they call `ensure_running()?`; it also exposes read-only `routed_envelope_delivery_state(&self, target, envelope_id)`, which is the right probe for whether hub calls reached CoreDaemon's router.
- `CoreDaemon` routed-envelope state uses the same queue config currently cloned into HubRuntime's local router, so removing the local router should not change queue limits except by eliminating the duplicate owner.
- Lua and native HubRuntime coordination already share the same hub-local `Arc<Mutex<RoutedEnvelopeRouter>>` today. A Lua-publish/native-drain test would pass on main and is not discriminating. The real defect is that HubRuntime's local router is used while CoreDaemon's router is unused from the hub path.
- Lua plugin workers must not hold raw core routers or product-policy state. The smallest implementation is to change `SharedCoreDaemon` from private `Mutex<CoreDaemon>` to `Arc<Mutex<CoreDaemon>>`, wrap an `Arc` clone in a narrow `HubCoordinationBridge` exposing exactly publish/drain/acknowledge, and pass that bridge in `LuaPluginHostApi` instead of `SharedRoutedEnvelopeRuntime`.
- Do not copy the `session_templates.spawn` pending-queue/pump pattern for coordination. That pattern exists for hub policy, admission, metadata, and session-template side effects; routed-envelope publish/drain/ack is straight daemon delegation through an already mutex-protected `CoreDaemon`.
- Project Pipelines' immediate publish, drain, acknowledge sequence is intentionally a coordination proof. It should still return primitive cursor and delivery states after the routing change.
- CoreDaemon publish/drain/ack are `ensure_running()`-gated while the old local router was not, but this is only a latent risk through current hub APIs: `release_for_restart()` does not set `CoreDaemon.running = false`, and HubRuntime does not expose the all-sessions `shutdown(None, ..)` path. Do not add a hub full-shutdown path or a shutdown-gating test for this ticket.
- No human question blocks the plan. If implementation discovers `CoreDaemon` lacks the required methods in the checked-out dependency, it should stop and ask before adding another hub-local router or compatibility path.

## Affected Surfaces And Files

- `src/runtime.rs`: change `type SharedCoreDaemon = Mutex<CoreDaemon>` to an `Arc<Mutex<CoreDaemon>>` shape or equivalent shared newtype; construct one CoreDaemon handle in `new`/`from_validated_state`; remove `routed_envelopes` field, constructions at the current `src/runtime.rs:123-132` and `181-190`, `routed_envelope_runtime()`, and the `lua_plugin_host_api` handoff of `routed_envelopes`; update routed-envelope facade methods to lock and call `core_daemon`; delete comments that currently name hub ownership.
- `src/lua_runtime.rs`: replace `SharedRoutedEnvelopeRuntime` and direct `RoutedEnvelopeRouter` imports with a narrow coordination bridge API; update `LuaPluginHostApi`; update `coordination_table` closures to call publish/drain/ack through that bridge instead of locking `RoutedEnvelopeRouter`.
- `src/client_api.rs`: likely no behavior change; keep native `PublishRoutedEnvelope`, `DrainRoutedEnvelopes`, `AcknowledgeRoutedEnvelope`, and `NotifySession` request handling as the public entry path into `HubRuntime`.
- `src/daemon_transport.rs`: likely no behavior change; existing `PostMessage`, `ReceiveMessages`, and `AckMessage` already route through `HubClientApi` and should remain the native MCP/daemon adapter path.
- `src/lib.rs`: keep or lightly adjust the facade exposure text only so it matches the now-true implementation; the existing assertion around the exposure string should remain valid unless naming changes are necessary.
- `tests/hub_lua_runtime_test.rs`: adjust direct construction at the current `tests/hub_lua_runtime_test.rs:1292-1298` to use the new bridge; add a CoreDaemon-observation regression that fails on main by proving hub-published and Lua-published envelopes do not currently appear in CoreDaemon's router.
- `tests/hub_mcp_test.rs`: keep native MCP coordination and restart-loss tests; keep the actual Project Pipelines test `mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools`; do not cite nonexistent test names.
- `tests/hub_runtime_test.rs`: add direct runtime-level coverage for a thin `HubRuntime::routed_envelope_delivery_state` accessor if the implementer places the CoreDaemon-observation regression there instead of in `hub_lua_runtime_test`.
- `README.md`, `docs/lua-plugin-abi.md`, `examples/project-pipelines/README.md`, and `docs/adr/local-runtime-dogfood-readiness.md`: update wording to a single CoreDaemon-owned routed-envelope owner and one process-memory restart story. Specifically fix `README.md:607-609` parallel-composition wording and `examples/project-pipelines/README.md:54-58` if it still suggests Rust exposes a local routed-envelope helper separate from daemon ownership.
- `examples/project-pipelines/plugin.lua`: only touch if the Lua helper return shape changes; prefer preserving the plugin code unchanged.

## Implementation Shape

1. Inspect `CoreDaemon` routed-envelope method signatures and current `HubRuntime` compile surface.
2. Convert CoreDaemon sharing inside HubRuntime to one `Arc<Mutex<CoreDaemon>>` handle. This is a required type change because `LuaPluginHostApi` holds shared handles while the current `SharedCoreDaemon` is only a bare private `Mutex<CoreDaemon>`.
3. Introduce a narrow Lua coordination bridge type in Rust that owns an `Arc` clone of the same CoreDaemon handle and exposes only publish, drain, and acknowledge. This keeps local clients over `HubRuntime`, avoids raw core router exposure, and keeps the Lua helper a reusable framework primitive rather than Project Pipelines product policy.
4. Replace `LuaPluginHostApi.routed_envelopes` with the bridge and update `HubRuntime::lua_plugin_host_api`.
5. Delete the HubRuntime-owned `RoutedEnvelopeRouter` field, all construction of the local router, `routed_envelope_runtime()`, and `SharedRoutedEnvelopeRuntime`. Do not keep a test-only seam; this is a cold-turkey removal because dual routing ambiguity is the defect.
6. Preserve JSON/Lua return shapes for `RoutedEnvelopePublishOutcome`, `RoutedEnvelopeDrainOutcome`, and acknowledgement state so Project Pipelines and MCP tests do not need product-level rewrites.
7. Add a thin `HubRuntime::routed_envelope_delivery_state(target, envelope_id)` method that delegates to `CoreDaemon::routed_envelope_delivery_state`. This is read-only, mirrors an existing daemon method, and is allowed by the ticket's "thin daemon methods if missing" non-scope boundary.
8. Add the discriminating test before or with the change: publish one envelope through `HubRuntime::publish_routed_envelope` and one through Lua `botster.coordination.publish`, then assert `HubRuntime::routed_envelope_delivery_state` returns `Some(state)` for both. On main both return `None` because hub calls never write into CoreDaemon's router; after the change both return `Some`.
9. Update docs after code so the documented path matches the production entry point: MCP/native and Lua/plugin coordination both delegate to CoreDaemon routed-envelope APIs.

## Risks

- Deadlock risk: plugin invocation already calls back into hub-owned helpers. The bridge must avoid holding plugin lifecycle or unrelated mutex guards while calling into `CoreDaemon`.
- Worker boundary risk: Lua helper calls must remain bounded and return useful errors if the coordination bridge fails or the core daemon mutex is poisoned.
- Semantic drift risk: changing the bridge may accidentally alter envelope source endpoint, target encoding, cursor handling, limits, or ack state serialization.
- Lifecycle behavior risk: CoreDaemon publish/drain/ack are `ensure_running()`-gated while the old local router was not. Through current HubRuntime paths this is latent because hub never sets `running = false`, but future full-daemon shutdown plumbing must remember coordination will fail closed.
- Test false-positive risk: source scans, current MCP tests, and Lua/native round-trip tests can pass on main. Acceptance must include a CoreDaemon-router observation test that fails before the change and passes after it.
- Restart wording risk: because queues remain process memory, docs must say they are lost after daemon restart and must not imply durability because sessions and hub state are worker/file backed.

## Acceptance Checks And Tests

- Source invariant: `rg -n "RoutedEnvelopeRouter|routed_envelopes|SharedRoutedEnvelopeRuntime" src tests` shows no HubRuntime-owned product router remains. Any remaining mention must be core imports, docs, or intentionally renamed bridge code, not a second queue table.
- `cargo fmt`.
- Add a new discriminating test, preferably in `tests/hub_lua_runtime_test.rs`, with a name like `lua_and_native_coordination_publish_into_coredaemon_router`. It must publish one envelope through `HubRuntime::publish_routed_envelope` and one through `botster.coordination.publish`, then assert a new thin `HubRuntime::routed_envelope_delivery_state` accessor reports `Some(state)` for each. Record that this test fails on main before implementation because CoreDaemon's router sees neither envelope.
- `./test.sh --test hub_lua_runtime_test lua_and_native_coordination_publish_into_coredaemon_router`.
- `./test.sh --test hub_mcp_test mcp_native_coordination_tools_route_messages_through_daemon_envelopes`.
- `./test.sh --test hub_mcp_test mcp_routed_envelopes_are_not_restart_durable_today`.
- `./test.sh --test hub_mcp_test mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools`.
- If touched shared public structs or generated protocol DTOs change, run the relevant broader workspace gate (`cargo test` or `cargo clippy`) and document why it was needed.
- Documentation/source cleanup check: `rg -n "separately composes|HubRuntime owns coordination routing|routed_envelope_runtime|SharedRoutedEnvelopeRuntime|second in-memory|parallel inbox|local router" README.md docs/adr docs/lua-plugin-abi.md docs/client-protocol.md examples src tests` should return no stale parallel-inbox or local-router hits. Do not include `docs/plans/**`, because this plan intentionally discusses the removed concepts.

## Vault Gaps Worth Capturing

- Capture if implementation discovers a reusable pattern for synchronous Lua worker helpers that need to share a hub-owned daemon handle without creating new local state.
- Capture if `CoreDaemon` routed-envelope methods differ from README/lib facade claims, because that would be a durable contract drift between hub docs and the compiled core dependency.
- No new durable knowledge is known yet from planning alone beyond the already-loaded [[hub coordination envelopes need one sanctioned owner]] note.
