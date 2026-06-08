# TUI UiNode Semantic Event Routing Plan

## Context Loaded

- Pipeline context: ticket `ticket_1780939863_575059`, run `run_1780957206_340541`, active Plan step `botster_plan`, gate `botster_plan_gate`.
- Ticket dependency: "Implement botster-hub TUI UiNode renderer scaffold over the core contract" is closed.
- No prior artifacts, findings, open questions, or question answers were present when planning started.
- Required playbooks and vault notes loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[plan steps need reviewable plan artifacts]], [[nested rich tuis lose scrolling when botster consumes mouse reports or control keys]], [[terminal accessory reattach must restore nested tui input passthrough]], and [[botster tui attach must explicitly pull core entities after subscribing]].
- Repo context inspected: `src/tui.rs`, `tests/hub_daemon_lifecycle_test.rs`, `Cargo.toml`, and the locked `botster-core` UI contract source.
- Checklist evidence: Project Pipelines run checklist `checklist_1780957346_121469` records vault notes loaded, no convention conflicts, planning verification evidence, and capture decision.

## Scope

- Add a TUI input routing layer in `src/tui.rs` that resolves keyboard and mouse events against rendered UiNode regions by stable `UiNodeId`.
- Preserve the existing production entry point: `run()` polls crossterm events and should call the new router instead of the current hard-coded `handle_key` path.
- Extend the renderer or companion layout pass so rendered nodes produce hit-testable regions for panels, lists, list items, forms, dialogs, menus, scroll areas, and the terminal view.
- Route semantic actions from action-bearing nodes (`Button`, `IconButton`, `MenuItem`, and form submit/cancel/reset affordances) using the existing core `UiActionId`, `UiActionPending`, and `UiActionResult` vocabulary. If no daemon request currently carries these actions, add the smallest typed daemon request/response needed to deliver the semantic action over the existing client API boundary.
- Implement keyboard-first traversal and activation for currently rendered nodes: list selection, Enter/Space activation, Escape cancel/detach behavior, Tab/Shift-Tab focus movement, arrows for lists/menus/select options, form field editing, submit, cancel, and scroll-area navigation.
- Implement mouse dispatch for click selection/activation, wheel scrolling, pane/session selection, dialog/menu interaction, and terminal-view passthrough.
- Preserve raw byte forwarding to the focused `TerminalView`, including ordinary text, Enter, Backspace, control keys not owned by Botster chrome, and mouse reports when the terminal owns mouse input.
- Keep stale attached-session behavior from the existing UnknownSession drain handling: one actionable detach/refresh row, no repeated event spam.

## Non-Scope

- Do not redesign the core UiNode schema or add broad renderer primitives unless the pinned core contract cannot express a ticket-required action.
- Do not move workflow/product policy into core Lua or Project Pipelines plugin code.
- Do not replace the TUI renderer, add a new UI framework, or refactor unrelated daemon transport code.
- Do not implement browser/React SPA event routing.
- Do not send renderer-private raw mouse events through the core UI contract. Raw input belongs only to an explicitly focused terminal view.

## Assumptions And Unknowns

- Assumption: `src/tui.rs` is the only required production TUI surface for this ticket because it owns rendering, input polling, terminal attach, daemon requests, and scripted TUI tests in this repo.
- Assumption: stable node ids already emitted by `TuiClient::ui_tree()` are sufficient for the local TUI scaffold; plugin-owned external trees may need the same router later but are not present in this repo.
- Assumption: implementation can keep most routing local and map local hub-authored nodes to existing `TuiClient` methods (`attach_selected`, `detach`, `send_input`, `resize`, refresh/shutdown helpers).
- Unknown: the locked core revision exposes `UiAction`, `UiActionPending`, and `UiActionResult`, but not a full outbound `UiActionRequest` envelope. Implementer must confirm whether a daemon route already exists before adding one.
- Unknown: terminal mouse-mode state is not visible in the inspected `src/tui.rs` scaffold. If no mode shadow exists in this repo, terminal-view mouse passthrough should be implemented with explicit ownership rules and tests that avoid claiming full nested-TUI mode restoration beyond this hub scaffold.
- Unknown: current renderer records region ids but not rectangles. Implementer must choose the smallest layout recording approach that stays consistent with ratatui rendering.

## Botster Layers Touched

- TUI: primary layer, including render-region metadata, keyboard routing, mouse routing, focus state, and scripted test harness.
- Rust hub/client API: only if semantic UiAction dispatch lacks a typed daemon request.
- Tests: Rust unit tests for routing and daemon lifecycle integration tests for runtime path proof.
- Docs: this plan artifact; no plugin README update expected unless implementation changes Project Pipelines plugin UI contract.

## Affected Surfaces And Files

- `src/tui.rs`
  - `run()` event loop: route `Event::Key`, `Event::Mouse`, and `Event::Resize`.
  - `TerminalModeGuard`: mouse capture is already enabled; may need bracketed paste or focus capture only if required and tested.
  - `TuiClient`: focus/selection/form/edit/scroll state plus semantic routing methods.
  - `TuiUiRenderer`: region collection/hit testing, list item and terminal rectangles, scroll area rendering.
  - Test module: add router and hit-test unit coverage.
- `tests/hub_daemon_lifecycle_test.rs`
  - Extend scripted TUI proof for keyboard-first operation, mouse selection/click behavior, terminal forwarding, scroll behavior, and stale session suppression.
- Potentially `src/daemon_transport.rs`, `src/client_api.rs`, `src/lib.rs`, or related daemon request/response declarations if a typed semantic UiAction request does not already exist.
- Potentially `docs/lua-plugin-abi.md` only if implementation exposes a new public plugin ABI path, which is not expected for the surgical path.

## Risks

- Terminal fidelity regression: outer TUI shortcuts or mouse handling could swallow bytes that a nested terminal app expects.
- Coordinate drift: render and hit-test layout can diverge if the router rebuilds rectangles differently from ratatui draw.
- Stale state spam: click/scroll dispatch against a vanished session could reintroduce repeated UnknownSession operator errors.
- Contract ambiguity: adding an action request route in hub instead of core may be acceptable for this scaffold, but a broad core contract change would exceed the smallest-change intent.
- Form complexity: full form edit/submit/cancel behavior can grow quickly; implement only the field types present in `UiNodeKind` and cover them with focused tests.

## Acceptance Checks And Tests

- Unit tests in `src/tui.rs`:
  - Keyboard focus traversal reaches list items, action nodes, fields, dialogs, menus, scroll areas, and terminal view by stable node id.
  - Enter/Space activates list rows/buttons/menu items and emits semantic action identity or calls the mapped local action.
  - Text input/textarea editing, checkbox toggle, select option movement, submit, cancel, and validation failure rendering work through the router.
  - Mouse hit testing maps coordinates to the expected stable `UiNodeId`; click selects/activates rows and buttons; wheel scrolls scroll areas.
  - Terminal-view focus forwards raw key bytes and terminal-owned mouse bytes instead of converting them into renderer-private core events.
- Integration/scripted tests in `tests/hub_daemon_lifecycle_test.rs`:
  - Keyboard-first selection/attach/send-input still works through the production scripted TUI client.
  - Mouse row selection/click dispatch selects or attaches a session through the same runtime path.
  - Scroll behavior changes visible router state without sending raw renderer mouse events through the UI contract.
  - Terminal input forwarding proves typed characters, Enter, Backspace, representative control keys, and mouse-mode bytes reach the child PTY when terminal view owns focus.
  - Stale attached-session drain still clears active session/subscription once and does not spam generic UnknownSession rows.
- Commands for implementer:
  - `./test.sh --unit` or the repo-approved equivalent if the harness expects `BOTSTER_ENV=test`.
  - Targeted daemon lifecycle filter through `./test.sh --integration -- <test-function-substring>` or `cargo test --test hub_daemon_lifecycle_test <test-function-substring>` only if the local repo convention confirms the latter is safe.
  - `cargo fmt`.

## Runtime Path Proof

The changed user path must be the interactive TUI path, not only helper methods. Evidence should show `run()` consuming real `crossterm::event::Event::Key` and `Event::Mouse`, routing through the new UiNode router, and then calling `TuiClient` methods or daemon requests. Tests should exercise the same router used by `run()`; helper-only tests are not sufficient.

## Worktree And Target Assumptions

- Worktree is the pipeline-provided ticket worktree for run `run_1780957206_340541`; plan artifacts avoid absolute local paths.
- Target is the explicit Project Pipelines run target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pre-existing dirty files observed before this plan: `.gitignore`, `.env`, and `mise.local.toml`; implementation should not revert or depend on those unrelated changes.

## Pipeline Gates And Artifacts

- This file is the repo-visible Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Gate evidence should attach this plan, checklist id `checklist_1780957346_121469`, context loaded, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks, and vault gaps.
- Plan Review should reject implementation proposals that only prove code exists without proving the production `run()` event path changed.

## Vault Gaps Worth Capturing

- If implementation discovers a settled local pattern for TUI UiNode region metadata and hit testing, capture it as a Botster TUI note.
- If implementation has to add or intentionally defer a typed `UiActionRequest` envelope because the locked core only has action/pending/result structs, capture the contract boundary decision.
- If terminal mouse-mode ownership cannot be proved from current hub TUI state, capture the gap between this scaffold and the richer terminal-mode restoration notes.
