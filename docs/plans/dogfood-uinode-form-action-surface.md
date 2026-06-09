# Dogfood UiNode Form Action Surface Plan

## Context Loaded

- Project Pipelines context loaded for run `run_1780963808_368977`, step `botster_plan`, gate `botster_plan_gate`, ticket `ticket_1780939863_480104`.
- Ticket dependencies are closed: "Implement botster-hub TUI UiNode renderer scaffold over the core contract" and "Add semantic keyboard and mouse event routing for TUI UiNode surfaces".
- No prior artifacts, findings, reviews, open questions, or question answers were present.
- Required vault context loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context loaded: `src/tui.rs`, `examples/project-pipelines/plugin.lua`, `examples/project-pipelines/README.md`, `tests/hub_mcp_test.rs`, `Cargo.toml`, and the pinned `botster-core` UiNode/action result schema.
- Project Pipelines checklist discipline: the run checklist was created after an initial plugin worker timeout; checklist evidence should be kept in both the checklist and this plan/gate evidence.

## Scope

- Dogfood Project Pipelines ticket creation as the one real hub/plugin surface because `project_pipelines.create` already exists in the local plugin and is exercised through the production `botster-hub mcp-serve` plus daemon/plugin-worker path.
- Add a plugin-owned UiNode form description for creating a constrained local ticket, likely next to the Project Pipelines Lua policy in `examples/project-pipelines/plugin.lua` unless the repo already has a more specific surface registration seam.
- Include fields needed by the existing create path: ticket title and pipeline id. Add only narrow optional fields if needed to prove normalized returned values, for example trimming title and defaulting pipeline id.
- Move validation for the form into plugin-owned Lua policy: required title, allowed/simple pipeline id, and structured failure details that include field-level and form-level errors.
- Preserve the shared UiNode contract. Use existing node kinds (`Form`, `TextInput`, `Select` or `TextInput`, `Button`, `Text`, `Panel`) and action result structures before considering core schema expansion.
- Wire the TUI to render and act on the real plugin-authored form through generic UiNode and action result handling. The changed runtime path must flow through the existing `run()` event loop, `route_event`, hit regions, and renderer rather than one-off Project Pipelines TUI screens.
- Show honest action state in TUI: pending/submitted state if available in the local action path, validation failures beside the relevant field/form, and success feedback with the created ticket id/title/status from the plugin action result.
- Update `examples/project-pipelines/README.md` only for the local UI contract or manual dogfood flow that future agents need at the plugin boundary.
- Add focused automated tests for the plugin validation/action behavior and the TUI render/event feedback path, then run the repo-approved harnesses.

## Non-Scope

- Do not extract first-party plugins into separate repos.
- Do not implement full monolith Project Pipelines parity, GitHub/PR automation, cloud/Rails/WebRTC/browser marketplace surfaces, or full agent spawn/worktree orchestration.
- Do not add broad new UiNode primitives, renderer abstractions, or a second Project Pipelines implementation in Rust.
- Do not build a general form engine beyond the field types and action behavior needed by this one real dogfood workflow.
- Do not persist PII or include local absolute worktree paths in committed artifacts.

## Botster Layers Touched

- Plugin: `examples/project-pipelines/plugin.lua` for the real form, validation, normalized values, and action result payloads.
- TUI: `src/tui.rs` for generic rendering/action-state behavior required to complete the form and display results.
- MCP/runtime tests: `tests/hub_mcp_test.rs` or a focused companion test for the production daemon/MCP/plugin-worker path.
- Docs: `examples/project-pipelines/README.md` if implementation changes the plugin UI contract or manual verification steps.
- Plan artifact: this file.

## Assumptions And Unknowns

- Assumption: "Prefer package/plugin configuration or Project Pipelines ticket creation" can be satisfied by Project Pipelines ticket creation because the local plugin already exposes `project_pipelines.create` through the production daemon-backed MCP path.
- Assumption: field errors and form errors can be represented as plugin action result payload content plus TUI mapping to field/form node ids, without changing the pinned core UiNode schema.
- Assumption: normalized returned values means the success result should expose the canonical ticket fields after plugin validation/defaulting, not merely echo raw input.
- Unknown: whether the current hub has a plugin surface registration API for UiNode snapshots in this scaffold. If not, keep the surface local to the TUI/plugin dogfood path and document why it is intentionally scaffold-level.
- Unknown: whether action pending state already has a daemon/plugin transport path. If not, implement the smallest honest "submitted/result" state in the TUI and avoid inventing durable pending primitives.
- Worktree/target assumption: downstream agents should use target `tgt_7e208a0c76a44980a83b63af976b1f22` and the assigned run worktree, not an ambient checkout.

## Affected Surfaces And Files

- `examples/project-pipelines/plugin.lua`: add UiNode form/action policy and server/plugin-owned validation for ticket creation.
- `examples/project-pipelines/README.md`: document the local Project Pipelines UiNode form/action dogfood contract if changed.
- `src/tui.rs`: render form, field/form errors, success feedback, and route submit/edit events through generic UiNode/action behavior.
- `tests/hub_mcp_test.rs`: extend the existing Project Pipelines daemon/MCP test or add a focused test that proves validation failure and success through the production plugin path.
- `tests/hub_local_dogfood_test.rs` or TUI unit tests in `src/tui.rs`: prove the TUI path renders the real form and visible action result feedback.
- `docs/plans/dogfood-uinode-form-action-surface.md`: durable plan artifact.

## Risks

- The core UiNode schema currently allows only narrow props; ad hoc `field_errors` props on fields would violate validation. Mitigation: carry structured errors in action result payloads and map them by stable node ids in the TUI.
- The TUI could accidentally become a Project Pipelines-specific screen. Mitigation: keep Project Pipelines policy in Lua and make Rust changes generic over UiNode/action result data.
- Tests that only render helper fixtures would not prove the runtime path changed. Mitigation: include evidence from the production `mcp-serve`/daemon/plugin path and from the TUI event/render path used by `run()`.
- Lua `ipairs` nil truncation can drop later UI nodes if optional entries are inserted inline. Mitigation: build optional node arrays with guarded insertion.
- PluginDb persistence errors can mask validation behavior. Mitigation: test validation failure before persistence and success with persisted current-context evidence.
- Checklist writes can time out in Project Pipelines. Mitigation: preserve vault/checklist evidence in this plan and gate evidence.

## Acceptance Checks And Tests

- Automated plugin/runtime: run `./test.sh mcp_serve_lists_calls_and_reloads_project_pipelines_plugin_tools` or the narrowed equivalent after extending it to assert:
  - `tools/list` exposes the Project Pipelines form/action surface if exposed through MCP, or the create action can be invoked through the same production plugin handler used by the surface.
  - validation failure returns `ok=false` with field-level and form-level error data and does not create a ticket.
  - success returns `ok=true` with normalized ticket values and the ticket appears in `project_pipelines.current_context` after daemon/plugin persistence.
- Automated TUI: run focused `src/tui.rs` tests proving:
  - the real Project Pipelines form UiNode validates against the core schema.
  - field/form errors and success action results render visibly in a ratatui test frame.
  - key or mouse submit routes through the same `route_event`/hit-region path used by `run()`, not a helper-only path.
- Broader guardrails: run `cargo test --lib tui_ui_renderer` or the exact focused filter added by implementation, then the relevant integration filter through `./test.sh` if that is the repo-approved wrapper for daemon tests.
- Manual dogfood evidence: start the daemon with a temp data dir, enable `examples/project-pipelines`, open the TUI, submit the form once with an invalid title and once with a valid title, and record that validation failure and success feedback are visible.
- PII/artifact check: scan the committed plan/docs for local home paths before review.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this plan and record the loaded vault notes, convention conflict result, verification plan, and capture decision.
- Implement gate should require committed code, a linked PR if the pipeline policy requires one, and exact test/manual evidence for both validation failure and success paths.
- Review should reject helper-only tests or unwired code; the production entry point must be named in the implementation report.

## Convention Conflicts

None found. The plan keeps Project Pipelines workflow policy in the plugin, keeps Rust TUI behavior generic over shared UiNode/action contracts, uses the existing daemon/MCP/plugin-worker path, avoids new core primitives unless the pinned contract cannot express the required behavior, and keeps repo artifacts path-neutral with vault context cited by note title.

## Vault Gaps Worth Capturing

- Capture a Botster note if implementation settles the representation for form-level and field-level validation errors over `UiActionResult.payload` without extending the core UiNode schema.
- Capture a TUI note if a reusable pattern emerges for generic form editing/submission over UiNode hit regions.
- No new capture is needed for the checklist worker timeout unless it recurs beyond the already documented timeout and fallback path.
