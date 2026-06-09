# Dogfood UiNode Form Action Surface Plan

## Context Loaded

- Project Pipelines context loaded for run `run_1780963808_368977`, returned step `botster_plan`, gate `botster_plan_gate`, ticket `ticket_1780939863_480104`.
- Ticket dependencies are closed: "Implement botster-hub TUI UiNode renderer scaffold over the core contract" and "Add semantic keyboard and mouse event routing for TUI UiNode surfaces".
- Prior Plan Review `review_1780964407_730256` returned changes required with six findings: two blockers on missing plugin-to-TUI surface/action transport and scaffold fallback, one high finding on helper-only action-result tests, one medium finding on structured error mapping, one low finding on pending/submitted wording, and one info finding on recurring checklist worker timeouts.
- Required vault context loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], and [[botster tui uinode event routing captures hit regions during draw]].
- Repo context loaded: `src/tui.rs`, `src/lua_runtime.rs`, `src/runtime.rs`, `src/daemon.rs`, `src/daemon_transport.rs`, `src/client_api.rs`, `examples/project-pipelines/plugin.lua`, `examples/project-pipelines/README.md`, `tests/hub_mcp_test.rs`, `Cargo.toml`, and the pinned `botster-core` UiNode/action result schema.
- Project Pipelines checklist discipline: the run checklist was created after an initial plugin worker timeout; checklist evidence should be kept in both the checklist and this plan/gate evidence.

## Scope

- Dogfood Project Pipelines ticket creation as the one real hub/plugin surface because `project_pipelines.create` already exists in the local plugin and is exercised through the production `botster-hub mcp-serve` plus daemon/plugin-worker path.
- Add a plugin-owned UiNode surface route for creating a constrained local ticket, next to the Project Pipelines Lua policy in `examples/project-pipelines/plugin.lua`. The form must be authored by the plugin, not hardcoded in the TUI.
- Build the missing end-to-end transport: plugin declares a `surface_route` handler that returns the form UiNode; daemon/client API carries that plugin-authored UiNode snapshot to the TUI; TUI submits a typed semantic action; runtime invokes the plugin action handler; the plugin action returns a `UiActionResult`; TUI renders that result.
- Include fields needed by the existing create path: ticket title and pipeline id. Add only narrow optional fields if needed to prove normalized returned values, for example trimming title and defaulting pipeline id.
- Move validation for the form into plugin-owned Lua policy: required title, allowed/simple pipeline id, and structured failure details that include field-level and form-level errors.
- Preserve the shared UiNode contract. Use existing node kinds (`Form`, `TextInput`, `Select` or `TextInput`, `Button`, `Text`, `Panel`) and action result structures before considering core schema expansion.
- Wire the TUI to render and act on the real plugin-authored form through generic UiNode and action result handling. The changed runtime path must flow through the existing `run()` event loop, `route_event`, hit regions, and renderer rather than one-off Project Pipelines TUI screens.
- Show honest action state in TUI using the actual contract: `UiActionStatus::Success` and `UiActionStatus::Failure`. A short TUI-local transient "submitting" affordance is allowed only as presentation state; durable pending/submitted action statuses are out of scope because the core contract has no such variant.
- Map structured validation exactly: plugin action returns `field_errors` keyed by stable UiNode id plus `form_errors` as strings; the action layer maps that to `UiActionStatus::Failure`, sets `error` to a human summary, and carries `{ field_errors, form_errors }` in `UiActionResult.payload`. Do not add `field_errors` or `form_errors` props to form/input nodes, because core UiNode validation rejects unknown props.
- Update `examples/project-pipelines/README.md` only for the local UI contract or manual dogfood flow that future agents need at the plugin boundary.
- Add focused automated tests for the plugin validation/action behavior and the TUI render/event feedback path, then run the repo-approved harnesses.

## Non-Scope

- Do not extract first-party plugins into separate repos.
- Do not implement full monolith Project Pipelines parity, GitHub/PR automation, cloud/Rails/WebRTC/browser marketplace surfaces, or full agent spawn/worktree orchestration.
- Do not add broad new UiNode primitives, renderer abstractions, or a second Project Pipelines implementation in Rust.
- Do not build a general form engine beyond the field types and action behavior needed by this one real dogfood workflow.
- Do not accept a scaffold-only or TUI-local workflow fallback. If implementation cannot wire the real plugin surface/action transport, it must ask a human rather than silently shipping bespoke TUI workflow code.
- Do not add durable pending/submitted action-result statuses.
- Do not persist PII or include local absolute worktree paths in committed artifacts.

## Botster Layers Touched

- Plugin: `examples/project-pipelines/plugin.lua` for the real form, validation, normalized values, and action result payloads.
- Lua runtime: `src/lua_runtime.rs` for registering and invoking `surface_route` plus action handlers and marshalling UiNode/action-result payloads across the plugin worker boundary.
- Hub runtime: `src/runtime.rs` for typed helpers that render a plugin surface and dispatch a plugin semantic action through `PluginInvocationRequest` with real `surface_id` context.
- Daemon protocol: `src/daemon_transport.rs` and, if needed, `src/daemon.rs` for new daemon request/response variants that carry plugin surface snapshots and semantic action results.
- Local client API: `src/client_api.rs` if the typed client vocabulary needs plugin surface render/action operations in addition to raw daemon protocol.
- TUI: `src/tui.rs` for generic rendering/action-state behavior required to complete the plugin-authored form, dispatch actions through the daemon, and display plugin-produced results.
- MCP/runtime tests: `tests/hub_mcp_test.rs` or a focused companion integration test for the production daemon/plugin-worker path. MCP can remain verification support, but the primary dogfood route must be TUI -> daemon/client_api -> plugin, not mcp-serve-only.
- Docs: `examples/project-pipelines/README.md` if implementation changes the plugin UI contract or manual verification steps.
- Plan artifact: this file.

## Assumptions And Unknowns

- Assumption: "Prefer package/plugin configuration or Project Pipelines ticket creation" can be satisfied by Project Pipelines ticket creation because the local plugin already exposes `project_pipelines.create` through the production daemon-backed MCP path.
- Assumption: field errors and form errors must be represented as `UiActionResult.payload.field_errors` and `UiActionResult.payload.form_errors`, plus a flat `UiActionResult.error` summary, without changing the pinned core UiNode schema.
- Assumption: normalized returned values means the success result should expose the canonical ticket fields after plugin validation/defaulting, not merely echo raw input.
- Assumption: [[botster tui uinode event routing captures hit regions during draw]] makes a typed daemon semantic-action request the right boundary because `project_pipelines.create_ticket` cannot map to existing local TUI methods like select, attach, send input, resize, or detach.
- Unknown: exact naming of the daemon/client API operations. The implementation should choose clear typed names such as `PluginSurfaceRender` and `PluginSurfaceAction`, but the behavior must be end-to-end and production-wired.
- Unknown: whether `src/client_api.rs` or only `src/daemon_transport.rs` is the best public local-client vocabulary for this scaffold. The plan requires at least one typed local client path and tests must name the production entry point used.
- Worktree/target assumption: downstream agents should use target `tgt_7e208a0c76a44980a83b63af976b1f22` and the assigned run worktree, not an ambient checkout.

## Affected Surfaces And Files

- `examples/project-pipelines/plugin.lua`: add UiNode form/action policy and server/plugin-owned validation for ticket creation.
- `examples/project-pipelines/README.md`: document the local Project Pipelines UiNode form/action dogfood contract if changed.
- `src/lua_runtime.rs`: surface route/action handler registration and invocation marshalling.
- `src/runtime.rs`: hub runtime methods for plugin-authored UiNode surface render and semantic action dispatch.
- `src/daemon_transport.rs`: daemon protocol variants and response fields for plugin surface snapshots/action results.
- `src/daemon.rs`: daemon owner wiring as needed for enabled plugin surface/action requests.
- `src/client_api.rs`: typed client API vocabulary if implementation exposes plugin surface/action through the local client facade.
- `src/tui.rs`: render plugin-authored form, collect/edit values, route submit through daemon/client API, render field/form errors and success feedback from plugin-produced `UiActionResult`.
- `tests/hub_mcp_test.rs`: keep/extend Project Pipelines plugin validation coverage where useful.
- `tests/hub_local_dogfood_test.rs`, `tests/hub_runtime_test.rs`, or a focused integration test: prove the production daemon/plugin-worker surface/action path.
- TUI unit or scripted probe tests in `src/tui.rs`: prove the TUI dispatch/render path uses `route_event` and receives plugin-produced `UiActionResult`.
- `docs/plans/dogfood-uinode-form-action-surface.md`: durable plan artifact.

## Risks

- The core UiNode schema currently allows only narrow props; ad hoc `field_errors` props on fields would violate validation. Mitigation: carry structured errors in action result payloads and map them by stable node ids in the TUI.
- The TUI could accidentally become a Project Pipelines-specific screen. Mitigation: keep Project Pipelines policy in Lua and make Rust changes generic over UiNode/action result data.
- Tests that only render helper fixtures would not prove the runtime path changed. Mitigation: require a production round-trip test where a submit goes through `route_event` -> daemon/client API -> plugin worker -> `UiActionResult` -> TUI render.
- Lua `ipairs` nil truncation can drop later UI nodes if optional entries are inserted inline. Mitigation: build optional node arrays with guarded insertion.
- PluginDb persistence errors can mask validation behavior. Mitigation: test validation failure before persistence and success with persisted current-context evidence.
- Runtime surface transport can accidentally duplicate MCP tool dispatch. Mitigation: reuse `PluginInvocationRequest`/handler registration mechanics while adding surface/action-specific typed wrappers; do not create a second Project Pipelines engine in Rust.
- Checklist writes can time out in Project Pipelines. Mitigation: preserve vault/checklist evidence in this plan and gate evidence.

## Acceptance Checks And Tests

- Automated plugin/runtime: add or extend an integration test proving plugin surface/action transport through the production daemon and plugin worker:
  - enable `examples/project-pipelines` through package lifecycle;
  - request the Project Pipelines create-ticket surface through the typed daemon/client API;
  - assert the returned plugin-authored UiNode tree validates against core schema and includes a form with stable ids;
  - submit invalid values through the typed semantic-action request and assert a plugin-produced `UiActionResult::Failure` with flat `error` plus structured `payload.field_errors` and `payload.form_errors`, and no persisted ticket;
  - submit valid values and assert a plugin-produced `UiActionResult::Success` with normalized ticket payload and the ticket visible in `project_pipelines.current_context` or the equivalent plugin state read.
- Automated TUI: run focused `src/tui.rs` tests proving:
  - the real Project Pipelines form UiNode is rendered from plugin surface data, not from a TUI-hardcoded Project Pipelines form.
  - field/form errors and success action results render visibly in a ratatui test frame.
  - key or mouse submit routes through the same `route_event`/hit-region path used by `run()`, dispatches to the daemon/client API, and then renders the returned plugin-produced `UiActionResult`, not a hand-injected test result.
- Production entry point evidence: implementation report must name the exact daemon/client API method and request variant that carries plugin surface render/action, and tests must exercise that entry point.
- Broader guardrails: run `cargo test --lib tui_ui_renderer` or the exact focused filter added by implementation, then the relevant integration filter through `./test.sh` if that is the repo-approved wrapper for daemon tests.
- Manual dogfood evidence: start the daemon with a temp data dir, enable `examples/project-pipelines`, open the TUI, submit the form once with an invalid title and once with a valid title, and record that validation failure and success feedback are visible.
- PII/artifact check: scan the committed plan/docs for local home paths before review.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this plan and record the loaded vault notes, convention conflict result, verification plan, and capture decision.
- Implement gate should require committed code, a linked PR if the pipeline policy requires one, and exact test/manual evidence for both validation failure and success paths.
- Review should reject helper-only tests or unwired code; the production entry point must be named in the implementation report.

## Convention Conflicts

None after revision. The plan keeps Project Pipelines workflow policy in the plugin, keeps Rust TUI behavior generic over shared UiNode/action contracts, uses plugin worker invocation instead of duplicating Project Pipelines policy in Rust, adds the typed daemon semantic-action boundary required by [[botster tui uinode event routing captures hit regions during draw]], avoids new core primitives unless the pinned contract cannot express the required behavior, and keeps repo artifacts path-neutral with vault context cited by note title.

## Vault Gaps Worth Capturing

- Capture a Botster note if implementation settles the representation for form-level and field-level validation errors over `UiActionResult.payload` without extending the core UiNode schema.
- Capture a TUI note if a reusable pattern emerges for generic form editing/submission over UiNode hit regions.
- Capture or strengthen the existing checklist-timeout note during implementation: the timeout recurred in both Plan and Plan Review for this run, so it is now a run-level reproducible operational issue rather than a single occurrence.
