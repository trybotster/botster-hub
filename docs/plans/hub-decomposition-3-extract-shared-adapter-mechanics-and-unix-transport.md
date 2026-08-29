# Hub Decomposition 3: Extract Shared Adapter Mechanics And Unix Transport

## Revision History

Revision 2, Plan visit 2, run step `run_step_1787991666_830100`. It answers `review_1787991640_635230`, verdict `changes_required`:

- `finding_1787991640_610024` (high, product): shared close progress absorbed subscription route ownership. Fixed. `src/transport/shared/close_progress.rs` now holds the progress value, its empty constructor, and a transport-neutral slice accumulator only. `ClosedEventRoute`, `ClosedHandle`, the route-map traversal, suppression, classification, and `DaemonEvent` construction stay in `src/subscription/closed_events.rs`. Assumption 1, the shared file list, the changed-file list, the isolation lens, the ownership-identity lens, and acceptance check 4 are reconciled.
- `finding_1787991640_520721` (medium, product): the guard plan omitted `cfg(test)` tail coverage. Fixed. The plan loads [[a source scanner can stay in cfg test skip mode through end of file]], records the measured scanner state of every guard-list file at base commit, names `src/subscription/closed_events.rs` as already ending in skip mode at depth 2 with the two unbalanced needles that cause it, and adds acceptance checks 16 through 18 for a scanner final-state invariant, the needle repair, and one seeded-tail red arm per moved scanner input.
- `finding_1787991640_197590` (medium, product): proof-name preservation had no exact comparison. Fixed. Acceptance check 10a defines a base-to-HEAD `cargo test -- --list` leaf-name multiset comparison plus an explicit module-path rename map.
- `finding_1787991640_965581` (info, process): Plan gate and step completion evidence were empty. The evidence was submitted and echoed back by the gate, but the recorded summary was empty. Revision 2 reuses `artifact_1787991162_241958` and `checklist_1787991089_890309` and submits a non-empty gate summary with all five required fields.
- Review request: name the exact tests that cover the teardown matrix. Added as a table under `production_path_proof`.

Revision 1, Plan visit 1, commit `1bafcbe`.

## Target Repository And Target Id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1787894419_699597`. Run: `run_1787990653_757857`. Step: `botster_stack_plan`, run step `run_step_1787990653_209486` (first Plan visit).
- The pipeline resolved the target id with `list_spawn_targets`. That tool maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. This plan does not infer the repository from the process working directory.
- Blocking dependency `ticket_1787894416_777916` (Hub decomposition 2: extract admission and subscription ownership) is `closed`. No open blocking dependency remains.
- Base commit: `a45cf7b`. After `git fetch origin main`, `git rev-parse HEAD` and `git rev-parse origin/main` both return `a45cf7b`. The worktree is not behind `origin/main`.
- Tracked worktree is clean (`git status --porcelain` returns zero lines). Tracked `.gitignore` has 5 lines and needs no restore.
- The worktree path contains no `:` character. The official gate therefore runs with the default `target/` layout and no `CARGO_TARGET_DIR`.
- Per visit, the authoritative list of plan commits lives in that visit's gate evidence, not in this document.

## Repository Playbook Loaded

- [[botster-hub-playbook]] -- the repository ownership charter for `botster-hub`.

## Other Role And Surface Playbooks And Atomic Notes Loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Composition rule:

- [[botster playbooks compose role with changed surface overlays]] -- this ticket changes Rust transport and adapter paths, so it selects the runtime surface only.
- [[botster repository playbooks are ownership charters composed with role overlays]] -- the repository charter sits between the role router and the surface overlay.

Atomic notes that constrain this ticket:

- [[daemon transport extraction moves ownership before deleting the facade]] -- the frozen target directory map and the migration order. This ticket is migration steps 4 and the Unix half of step 5.
- [[Hub extraction must reduce ownership rather than only split files]] -- an extraction moves implementation, state, policy, and tests. File count proves nothing.
- [[hub moves must extend source scanning guard file lists]] -- a move can leave a fixed `include_str!` or `hub_source()` guard green while it no longer scans the moved code.
- [[a source scanner can stay in cfg test skip mode through end of file]] -- correct file inclusion is not sufficient; the `production_source` scanner can enter skip mode and omit every later line, so scanner-state coverage is a separate proof obligation.
- [[a known positive control proves a scan is live not that its pattern set is complete]] -- a seeded early match does not prove that the file tail is scanned.
- [[fixed source guard lists need one ablation per added file]] -- one representative destination arm cannot prove the other list entries.
- [[botster hub gravity must be watched before it becomes the new monolith]] -- the drift this decomposition answers.
- [[botster hub is a first party host profile over core]] -- Hub owns trusted product policy over policy-free Core.
- [[botster Hub Rust stays a trusted host kernel]] -- Hub Rust owns privileged boundaries, not duplicate runtime mechanisms.
- [[concrete terminal transports stay in hub until a second host needs them]] -- this ticket creates no transport crate and moves nothing to Core.
- [[core owns duplex terminal transport while Hub stays content blind]] -- the content-blind invariant that the moved adapter code must preserve.
- [[proposed Core transport adapters use bounded writes without policy queues]] -- the one-slot bounded write rule that the shared slot must keep.
- [[proposed Core publishes the transport adapter conformance harness]] -- both production adapters keep passing `assert_terminal_adapter_conformance`.
- [[terminal adapter traits must not reuse TransportIngress or TransportEgress]] -- the shared module must not invent a second transport trait family.
- [[terminal adapters emit coalesced writable and closed wakes]] -- the existing wake meaning that this move must preserve exactly.
- [[adapter accepted writes are not consumer flushed writes]] -- accepted-write accounting must not become flush accounting during the move.
- [[host reconciliation must not rewrite a completed Core adapter close reason]] -- the close-cause invariant that the shared close code must preserve.
- [[ShutdownSession suppresses exact route generations before Core teardown]] -- exact `(session_id, subscription_id, generation)` suppression stays in `subscription`, not in shared transport.
- [[ShutdownSession suppression live tests are not a red oracle]] -- suppression order relies on deterministic unit lanes.
- [[Unix mux host frames flush before new terminal slots]] -- the Unix mux scheduling contract that must survive the move.
- [[Unix mux host events are unsolicited control frames]] -- Unix close events carry no request pairing and no terminal-body inspection.
- [[Fair host-control writing selects already-admitted frames]] -- the Unix call site of `next_ready_host_control_class` moves, and its guard must follow.
- [[Client event holders are connection-scoped]] -- the holder identity that the moved connection cleanup must preserve.
- [[botster runtime teardown lenses]] -- loaded because this ticket moves adapter close mechanics, connection cleanup, and route close bookkeeping. See the runtime-teardown section.
- [[a regression test must be shown to go red with the fix reverted]] -- each retargeted negative guard needs a red arm on its new file.
- [[express scope limits as invariants not closed enumerations]] -- this plan states commit kinds as invariants, not a fixed commit count.
- [[integration tests should use public agent apis not crate-internal test-only helpers]] -- moved unit tests stay unit tests inside their new modules.
- [[a ui contract import line change costs one test line in each generic client]] -- a zero-DTO-change move must keep that downstream cost at zero.
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]] -- strict gates must run under Rust `1.97.0`.
- [[Hub official gates must not set CARGO TARGET DIR]] -- the official locked gate needs the default worktree `target/`.
- [[Hub suite runs prebuild the session worker before the locked test wrapper]] -- prebuild before `./test.sh --locked`.
- [[strict clippy can hide later crate diagnostics behind the first compile failure]] -- rerun the full workspace Clippy after each repair.

Required Botster planning context from [[botster-planner-playbook]]:

- [[botster-architecture]] -- the Botster domain map.
- [[cli-patterns]] -- Rust CLI, TUI, PTY, and terminal-layer constraints.

## Context Loaded

Project and ticket context:

- `project_pipelines_current_context` for run `run_1787990653_757857`.
- `project_pipelines_get_project` for `project_1787600579_585482`, including the frozen ownership statement, the frozen WebRTC topology, the decomposition order, and the delivery rules.
- The vault capture at `ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md`, vault commit `8ef01f56`. Its directory map names `transport/shared/{adapter_slot.rs, wake.rs, close_reason.rs, close_progress.rs}` and `transport/unix/{listener.rs, connection.rs, mux_write.rs, adapter.rs}`.

Repository context read at base commit `a45cf7b`:

- `src/lib.rs` module list, `architecture_summary` crate-export rows, and the `production_sources_reject_terminal_drain_and_snapshot_phase_decode` guard file list.
- `src/unix_terminal_adapter.rs` (709 lines) in full.
- `src/webrtc_terminal_adapter.rs` (734 lines) in full, plus a normalized text diff against the Unix adapter to separate identical mechanics from real differences.
- `src/subscription/closed_events.rs` (632 lines) in full.
- The symbol map of `src/daemon_transport.rs` (7445 lines) to separate Unix listener, connection, framing, and mux scheduling from control dispatch.
- `src/host_control_fair_write.rs` guard, `test.sh`, `docs/plans/`, and `docs/reports/` prior art.
- Both guard families, enumerated with `grep -rn "include_str!" src` and `grep -rn "hub_source(" tests`.
- The `production_source` scanner in `src/lib.rs`, replayed line by line over every file in its guard list to measure each file's `cfg(test)` skip state at end of file.

## Scope And Non-Scope

### In scope

Create `src/transport/` with two child directories and move existing mechanics into them without behavior change.

`src/transport/shared/`:

- `adapter_slot.rs` -- the one in-flight write slot shared by both production adapters: the `closed`, `host_closed`, `would_block` state, the `slot: Mutex<Option<Vec<u8>>>`, the `close_work` flag, and the `try_write`, `pressure`, `close`, `close_from_host`, `snapshot_active`, and `complete_active` operations.
- `wake.rs` -- the existing wake plumbing. It holds the permit-storing `AdapterWake` that `src/webrtc_terminal_adapter.rs` owns today, and a plain `Notify`-waiters wake for the Unix flavor, behind one narrow wake sink that the shared slot calls.
- `close_reason.rs` -- the internal close cause. It derives host-adapter close against Core-adapter close from the slot state and preserves first-cause semantics.
- `close_progress.rs` -- `ClosedEventSliceProgress` accounting only: the progress value with its existing fields, `empty_close_event_progress`, and a transport-neutral slice accumulator that tracks visited and classified counts against caller-supplied maxima and records the resume cursor. It names no route record, no handle trait, no suppression, and no DTO.

`src/transport/unix/`:

- `listener.rs` -- socket path preparation and rebinding, socket cleanup, the accept loop, connection rejection, and the socket client-id counter.
- `connection.rs` -- the accepted-connection driver, the client-side `DaemonConnection` with `request` and `stream_attach`, the connection terminal reason, the connection cleanup guard and cleanup path, connection task reaping, and the entity subscription connection path.
- `mux_write.rs` -- Unix framing and mux scheduling: `MuxWriteState`, `PendingMuxClass`, `PendingMuxFrame`, `MuxWrite`, response and terminal flush ordering, resumable partial writes, frame serialization, and the async frame read and write helpers.
- `adapter.rs` -- the whole of `src/unix_terminal_adapter.rs`, including `UnixConnectionMux`, rebuilt over the shared slot, shared wake, and shared close progress.

Also in scope:

- Move each moved unit test with its implementation.
- Retarget every source-scanning guard, in both guard families, onto the new files.
- Add the new files to the `src/lib.rs` forbidden-construct guard list and add a `transport` row to `architecture_summary`.
- Keep `src/webrtc_terminal_adapter.rs` at its current path, and rebuild it over the same shared slot, wake, and close progress.
- Keep the deduped mechanics limited to code that is semantically identical between Unix and WebRTC today.

### Explicitly out of scope

- No `src/transport/webrtc/` directory. Splitting `src/local_webrtc.rs` is a later ticket.
- No deletion of `src/daemon_transport.rs`, and no move of control dispatch, the owner loop, `serve_daemon` composition, `DaemonControlState`, `PendingRuntimeState`, pump phases, or any `*_response` helper. Those belong to the next decomposition ticket.
- No common connection mux across Unix and WebRTC. Each transport keeps its own mux type, its own route map, and its own flush policy.
- No Core wake contract, no dedicated DataChannels, and no Core pin change.
- No admission, route, grant, label, or product policy inside `src/transport/shared/`.
- No new configurability, no new abstraction beyond the four shared files, and no adjacent cleanup.
- No protocol, DTO, serde-name, or proof-name change.

## Repository Ownership Boundaries And Cross-Repository Dependencies

- `botster-hub` owns every changed file. The change is internal module topology inside one crate.
- Core keeps the `TerminalAdapter` contract, subscription identity, generations, and the conformance harness. This ticket consumes `botster_core::contract::terminal_adapter` and `botster_core_test_support::terminal_adapter` unchanged.
- `botster-hub-client` keeps the DaemonEvent DTO and the `TERMINAL_SUBSCRIPTION_CLOSED_*` reason constants. This ticket changes no DTO and no wire value.
- Hub admission keeps grants, labels, peer generations, and budgets in `src/admission/`. Hub subscription keeps pending close frames, suppression sets, and generation bookkeeping in `src/subscription/`.
- Cross-repository dependencies: none. `botster-web`, `botster-tui`, and `botster-hub-client` need no change, because no public export, DTO, or wire shape moves. The Cargo `botster-core` pin stays at its current revision.
- No new dependency ticket is required. If implementation discovers that a shared move needs a Core-side change, the Implementer must stop and register a dependency against the `botster-core` target rather than widen this run.

## Assumptions And Unknowns

Assumptions, each stated so Plan Review can reject them directly:

1. `ClosedEventSliceProgress` accounting means the progress value, its empty constructor, and a transport-neutral slice accumulator for visited counts, classified counts, the `more` flag, and the resume cursor. `ClosedEventRoute`, `ClosedHandle`, the route-map traversal, the suppression check, the classification call, and the `DaemonEvent` construction all stay in `src/subscription/closed_events.rs`. Subscription keeps the loop and calls the shared accumulator for its counters; shared transport never sees a route record. The loop order, the suppression check position, and the single wake after a non-empty batch stay unchanged.
   The progress value keeps its exact existing fields, including the `after_route` cursor tuple. That field is a resume cursor value, not a route record. The acceptance check 3 guard therefore forbids `ClosedEventRoute`, `ClosedHandle`, suppression symbols, and admission symbols inside `src/transport/shared/**`, and exempts only the pre-existing `after_route` field name.
2. Internal close reasons means the internal cause state and its derivation, which already live in the adapter inner. The mapping from that cause to the `TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER` and `TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER` wire constants stays in `src/subscription/closed_events.rs`, because those constants are `botster-hub-client` DTO values and shared transport must hold no DTO authority. If Plan Review reads the ticket the other way, moving that two-line mapping into `close_reason.rs` is a small, isolated change.
3. Existing wake plumbing means both current wake flavors move into `shared/wake.rs`. The Unix adapter keeps `Notify::notify_waiters` semantics with no stored permit. The WebRTC adapter keeps its stored permit and its `wait_for_write`. Converging Unix onto the permit wake would change scheduling behavior, and the ticket forbids that.
4. The Unix `deferred` flag, `defer_flush`, `clear_defer_flush`, and `clear_deferred_flushes` are Unix-only. They stay in `transport/unix/adapter.rs` and do not enter the shared slot.
5. The two mux types are not semantically identical and stay separate. `close_all` closes from host on Unix and closes plain on WebRTC. `snapshot_writes` filters deferred routes on Unix and closed routes on WebRTC. WebRTC alone owns `close_events_admitted` and `drop_pending_events`. Unix alone owns `has_unsent_mux_writes` and `notify()`. Deduplicating these would change behavior.
6. `serve_daemon` stays in `src/daemon_transport.rs` and calls into `transport/unix/listener.rs`. `serve_daemon` composes the runtime, the control channel, and the owner loop, which are control concerns owned by the next ticket.
7. The Unix client-side `DaemonConnection` belongs to `transport/unix/connection.rs`, because it is the Unix connection role rather than control dispatch.

Unknowns for the Implementer to resolve, each with a required action:

- Exact cut lines inside `src/daemon_transport.rs` between the connection driver and the control-request path. Resolve by symbol, not by line count. If a function serves both roles, leave it in `src/daemon_transport.rs` and record why in the implementation report.
- Whether `handle_entity_subscription_async` reads as connection role or control role. Current judgment: connection role, because it runs on the accepted-connection task. Record the decision.
- Visibility widening. Some moved items will need `pub(crate)`. Record every widened item; no item may become `pub`.

## Affected Surfaces And Files

Botster layer touched: Rust hub, transport and adapter modules only. No Lua, no SPA, no TUI, no MCP, no docs contract.

New files:

- `src/transport.rs` (or `src/transport/mod.rs`, matching the existing `src/admission.rs` and `src/subscription.rs` style), `src/transport/shared.rs`, `src/transport/shared/adapter_slot.rs`, `src/transport/shared/wake.rs`, `src/transport/shared/close_reason.rs`, `src/transport/shared/close_progress.rs`, `src/transport/unix.rs`, `src/transport/unix/listener.rs`, `src/transport/unix/connection.rs`, `src/transport/unix/mux_write.rs`, `src/transport/unix/adapter.rs`.

Deleted file:

- `src/unix_terminal_adapter.rs`, after its content moves to `src/transport/unix/adapter.rs`.

Changed files:

- `src/daemon_transport.rs` -- loses the listener, connection, framing, and mux-scheduling sections and their unit tests; keeps control dispatch.
- `src/webrtc_terminal_adapter.rs` -- adopts the shared slot, shared wake, and shared close progress; keeps its own mux, its `AdapterWake` usage, and its own tests.
- `src/subscription/closed_events.rs` -- keeps the ledger, the route record, the `ClosedHandle` trait, the route-map traversal, the suppression check, the classification call, and the `DaemonEvent` construction; consumes the shared progress value and the shared slice accumulator for its counters only.
- `src/lib.rs` -- module declarations, `architecture_summary` rows, the crate-root module list test, and the guard file list.
- `src/host_control_fair_write.rs` -- retarget the Unix call-site guard onto `transport/unix/mux_write.rs`.

Guard inventory that must be retargeted, enumerated at base commit `a45cf7b`:

Source-side (`include_str!` under `src/`):

- `src/lib.rs:1004` scans `daemon_transport.rs`; add every new `src/transport/**` file to that list.
- `src/unix_terminal_adapter.rs:700` scans itself; moves to `src/transport/unix/adapter.rs` and retargets.
- `src/host_control_fair_write.rs:158` scans `daemon_transport.rs` for the Unix fair-write call site.
- `src/subscription/closed_events.rs:591` scans `../daemon_transport.rs` for the `ShutdownSession` arm. That arm stays in `src/daemon_transport.rs`, so the path stays valid; confirm it, do not assume it.
- `src/daemon_transport.rs:6116`, `:6179`, `:6201` scan regions of their own file. Confirm each region's anchor symbols still live in that file after the move.

Test-side (`hub_source()` and `read_to_string(root.join(...))` under `tests/`):

- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:441` and `:555-557`.
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs:124-133` and `:173-176`.
- `tests/session_projection_owner_loop.rs:175-191`.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:771`.

Scanner-state coverage, measured at base commit `a45cf7b`:

The `production_source` helper in `src/lib.rs` enters skip mode at each `#[cfg(test)]` and leaves it when its brace counter returns to the prior level. It counts braces inside string literals. Replaying that exact algorithm over every file in the guard list shows that one file already ends with skip mode active: `src/subscription/closed_events.rs`, which enters skip at line 335 and reaches end of file at depth 2. The cause is two test string literals with unbalanced braces at lines 593 and 596, `"DaemonRequest::ShutdownSession { session_id } => {"` and `"DaemonRequest::Drain {"`, which contribute net `+1` each. No production text follows that test module today, so nothing is lost yet. This ticket rewrites that exact file and adds nine more scanned files, so the defect must be closed before the move, not after.

The plan therefore requires a scanner final-state invariant:

- Add a checked scanner entry point that reports whether skip mode is still active at end of file, and assert for every file in the guard list that skip mode is closed. A file that reaches end of file in skip mode fails the guard instead of silently reporting complete coverage.
- Repair `src/subscription/closed_events.rs` so its two test needles balance, by building each needle through concatenation rather than one literal. Recommended over rewriting the scanner to ignore braces inside string literals, because the invariant converts every future occurrence from silent blindness into a loud failure, and rewriting the scanner is a broader change than this ticket needs. Record the tradeoff in the implementation report.
- Seed a forbidden production construct in text after the final `#[cfg(test)]` block of each moved scanner input, one file at a time, and require the guard to fail. A control inside earlier scanned text does not prove the tail is scanned.

Guard failure modes differ, and the plan treats them differently:

- A guard that asserts `source.contains(...)` fails loudly when its target moves. Retarget it and rerun.
- A guard that asserts `!source.contains(...)` stays green and goes blind when its target moves. Retarget it, and prove it red on the new file by reverting the forbidden construct into that file.
- A guard that bounds a region with two anchor symbols goes blind when a function between the anchors moves out. Re-derive the region against the post-move file and prove the region still contains the code it protects.
- A guard whose scanner is stuck in `cfg(test)` skip mode at end of file reports complete coverage while scanning nothing after that point. Assert the scanner's final state and seed a tail construct per file.

## Risks

1. Silent guard blindness. A negative source guard keeps passing after its protected code leaves the scanned file. Mitigation: enumerate both guard families before and after the move, add each new file to the fixed lists, and require one red-on-revert ablation per added file entry.
2. Scanner-state blindness. `production_source` can stay in `cfg(test)` skip mode through end of file, so a correctly listed file can still be unscanned after its last test module. `src/subscription/closed_events.rs` is already in that state at base commit. Mitigation: the scanner final-state invariant, the needle repair in that file, and one seeded-tail red arm per moved scanner input.
3. Region-bounded guard blindness. `src/subscription/closed_events.rs` and `src/daemon_transport.rs` guards split their source between two anchor symbols. A move can leave the region syntactically valid but empty of the protected code. Mitigation: assert a positive anchor inside each region in addition to the negative assertions.
4. Accidental behavior change while deduplicating. The two adapters look alike but differ in wake permit storage, deferred-flush filtering, close-from-host on `close_all`, and the test-only `would_block` setter. Mitigation: assumptions 3 through 5 above, plus a normalized text diff of the two adapters recorded in the implementation report to show which differences the move preserved.
5. Ordering change in close-event classification. Splitting the traversal from the ledger can move the suppression check or the wake call. Mitigation: keep the whole loop in subscription, keep the suppression check at its current position, keep the single wake after a non-empty batch, and keep every existing unit test in `src/subscription/closed_events.rs` unchanged.
6. Cut-line drift into control dispatch. `src/daemon_transport.rs` mixes roles. Moving too much would collide with the next ticket and would break the frozen decomposition order. Mitigation: assumption 6, plus an explicit non-scope list.
7. Import churn producing a large diff that hides a semantic change. Mitigation: commit the move separately from every mechanical import repair, so review can read one move-only commit per slice.
8. Suite-load flakes in `hub_daemon_lifecycle`. The host must be quiet; process-global taint and default concurrency can produce failures unrelated to this change. Mitigation: run the official gate on a quiet host and classify any failure with its exact marker before calling it unrelated.
9. Public API leak. A moved item widened to `pub` would change the crate surface. Mitigation: no item becomes `pub`; `architecture_summary` records `transport` as `AlreadyInternal`.

## Runtime-Teardown Class

- `teardown_class_applies`: yes. The ticket moves adapter close mechanics, the close-cause state, connection cleanup, and route close bookkeeping. It is move-only, so every answer below is a preservation requirement, not a new design.
- `teardown_isolation`: The ownership set that dies with one Unix connection stays exactly what it is today: that connection's `UnixConnectionMux`, its bound adapter handles, its route map, and its pending close events. One connection's death must not touch another connection's mux, and must not touch any WebRTC peer. The shared slot holds no cross-connection state, and the shared accumulator holds only counters and a cursor, so neither introduces shared ownership.
- `teardown_bounds`: No close path may gain a wait. `close` and `Drop` must keep returning without waiting on socket I/O or a writer lock, and must keep using `try_lock` with the existing poisoned and would-block arms. The bounded slice keeps `PUMP_MAX_ADMISSIONS_VISITED`, `PUMP_MAX_CANDIDATE_CLASSIFICATIONS`, and `PUMP_MAX_ROUTE_ENTRIES_VISITED` as its hard stop. The `dying` flag keeps marking every route reported and returning empty progress.
- `late_message_matrix`: The move creates no new ownership-creating message. The existing surfaces keep their current tagging, rejection, and sweep:

| Surface | Owner tag today | Rejection after terminal failure | Residual sweep |
|---|---|---|---|
| Unix terminal bind | `(session_id, subscription_id, generation)` route key in the connection mux | `close_all` sets `dying`; a closed handle rejects `try_write` with `Closed` | `close_all` drains the route map and closes each handle from host |
| Unix entity subscription | connection-scoped holder | connection cleanup path on EOF or terminal reason | connection cleanup guard on task exit |
| Unix client events | connection-scoped holder | connection cleanup path | connection cleanup guard |
| Host close event | route key plus suppression set in `ClosedEventLedger` | exact-generation suppression before Core teardown | bounded slice marks each route reported once |
| WebRTC terminal bind | route key in the peer mux | `close_all` sets `dying` | unchanged by this ticket |

  The Implementer must not add, remove, or re-tag a row. Any change to this table means the move stopped being move-only.
- `production_path_proof`: The production path stays: Core calls `try_write` on the exact adapter, the connection flush loop reads `snapshot_writes` and calls `complete_active`, socket EOF or a terminal reason runs the connection cleanup guard, `close_all` closes each route handle, the bounded close slice classifies closed routes, and the mux emits `TerminalSubscriptionClosed`. Proof runs through the existing `tests/hub_daemon_lifecycle` lanes against a real daemon child, not through helper calls. The existing conformance harness proves the adapter laws for both flavors.

  The exact existing tests that cover this matrix, all of which must stay green and keep their names:

  | Matrix row | Covering tests |
  |---|---|
  | Unix terminal bind, host close | `host_adapter_close_emits_terminal_subscription_closed_for_one_route`, `terminal_subscription_closed_feature_does_not_raise_default_requirement` |
  | Unix terminal bind, Core close | `core_write_budget_hard_stop_emits_core_adapter_closed`, `failed_remove_session_does_not_suppress_later_core_close` |
  | Unix connection death and EOF sweep | `connection_death_and_detach_do_not_emit_terminal_subscription_closed`, `unix_eof_releases_exact_attach_occupancy_on_sibling_status`, `unix_spawn_then_eof_keeps_host_session`, and the three EOF ablations `unix_eof_leave_route_ablation_keeps_named_pair_on_status`, `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status`, `unix_eof_pair_only_detach_ablation_drops_replacement_owner_generation` |
  | Host close event suppression | `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`, `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings` |
  | Ownership identity and reused ids | `stale_generation_close_does_not_sweep_replacement_owner`, `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner`, `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route` |
  | WebRTC terminal bind and peer loss | `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`, `webrtc_terminal_adapter_host_close_emits_negotiated_terminal_subscription_closed`, `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`, `webrtc_terminal_adapter_detach_peer_death_process_exit_and_shutdown_do_not_emit_close_event`, `webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close`, `local_webrtc_peer_close_detaches_terminal_subscriptions` |
  | Sibling survival on successful close | `peer_close_leaves_sibling_peers_working`, `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` |

  Every name in this table is part of the check 10a inventory. A missing or renamed entry fails the move.
- `ownership_identity`: The stable owner id stays `(session_id, subscription_id, generation)` for routes, and the connection or peer identity for holders. A delayed sweep must still not delete a row now owned by a live replacement, because suppression and route keys carry the generation. The subscription traversal must keep taking the key from the map, not from a snapshot taken earlier.
- `sibling_fail_closed_policy`: On a successful close, siblings keep working; that is unchanged. On ultimate local WebRTC close failure, the current bounded sibling sacrifice policy stays as it is. This ticket must neither introduce sibling sacrifice on Unix nor relax the WebRTC policy.

## Acceptance Checks And Tests

Ownership checks, which prove this is an extraction and not a file split:

1. `src/unix_terminal_adapter.rs` no longer exists.
2. `src/daemon_transport.rs` contains no listener, connection, framing, or mux-scheduling implementation. Prove with a source guard that asserts the moved symbol names are absent from `src/daemon_transport.rs` and present in the new files.
3. `src/transport/shared/**` contains no admission, route, grant, label, or product-policy symbol. Prove with a new negative source guard over the shared directory, listing the forbidden identifiers, with one red-on-revert arm.
4. `src/transport/shared/**` declares no cross-transport mux type and no route record. Prove with a guard that asserts `ClosedEventRoute`, `ClosedHandle`, and any shared mux or route-registry symbol are absent from `src/transport/shared/**` and present in their owning modules, plus the fact that `UnixConnectionMux` and the WebRTC mux both still live in their own transport modules.
5. `architecture_summary` gains a `transport` row classified `AlreadyInternal`, and the crate-root module list test covers it.

Behavior-preservation checks:

6. Both production adapters pass the Core conformance harness: `production_unix_adapter_passes_core_conformance_harness` in its new home and the WebRTC equivalent.
7. Every existing unit test in `src/subscription/closed_events.rs` passes unchanged, including the keyed-suppression slice test, the exact-generation suppression test, and the empty-snapshot test.
8. Every existing `mux_write_resume_tests` case passes unchanged in `src/transport/unix/mux_write.rs`.
9. Every existing `tests/hub_daemon_lifecycle` Unix and WebRTC lane passes: `unix_terminal_adapter.rs`, `webrtc_terminal_adapter.rs`, `webrtc_proofs.rs`, `sessions.rs`, `shutdown.rs`, `subscription_ownership_baseline.rs`, and `event_plane_saturation.rs`.
10. No protocol, DTO, or serde name changes. Prove with `git diff` over `src/client_api_dto/`, `src/daemon/error.rs`, and every `serde` attribute, showing zero changes.
10a. Exact proof-name preservation. Capture the full test inventory at base commit `a45cf7b` and at the final HEAD with `cargo test --workspace --locked -- --list`, and store both. Compare the multiset of leaf test names, meaning the final path segment of each entry. The two multisets must be identical: no proof name may be removed, renamed, or reduced in count. Module paths do change for moved tests, so record a separate explicit rename map from each old module path to its new one, and require every entry in that map to keep the same leaf name. Any intentionally removed duplicate test must be listed by name with its reason; an unexplained difference fails the check.
11. The Core pin is unchanged. Prove with a `git diff` over `Cargo.toml` and `Cargo.lock` showing no `botster-core` revision change.

Guard checks:

12. Both guard families are enumerated before and after the move with `grep -rn "include_str!" src` and `grep -rn "hub_source(" tests`, and both enumerations are recorded in the implementation report.
13. Every new `src/transport/**` file appears in the `src/lib.rs` forbidden-construct guard list.
14. Each added guard list entry has its own red-on-revert ablation. One representative arm is not accepted.
15. Each region-bounded guard is re-derived after the move and gains a positive anchor assertion, so an empty region fails.
16. The scanner final-state invariant exists: every file in the `src/lib.rs` guard list is asserted to leave `cfg(test)` skip mode closed at end of file, and a file that ends in skip mode fails.
17. `src/subscription/closed_events.rs` leaves skip mode closed after the needle repair, proven by the invariant in check 16 going red on that file before the repair and green after it.
18. Each moved scanner input has its own seeded-tail red arm: a forbidden production construct placed after that file's final `#[cfg(test)]` block makes the guard fail. One shared arm is not accepted.

Gate commands, run from the worktree with `RUSTUP_TOOLCHAIN=1.97.0` and with `CARGO_TARGET_DIR` unset:

19. `rustc --version` recorded from the same shell, showing `1.97.0`.
20. `cargo fmt --all -- --check`.
21. `cargo clippy --workspace --all-targets --locked -- -D warnings`, rerun in full after each repair.
22. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` before the suite.
23. `./test.sh --locked` on a quiet host. Any failure must be matched to its own named marker before anyone calls it unrelated.

Downstream proof required by the charter:

24. This ticket changes no public export and no UI contract, so the generic-client cost stays zero. Record the zero-cost claim with the evidence for checks 10 and 10a rather than rebuilding `botster-tui` and `botster-web`. If any public export or DTO does change, the ticket has left move-only scope, and the Implementer must stop and re-plan.

Commit shape, expressed as invariants rather than a fixed count:

25. Each slice lands as a move-only commit that changes no behavior, followed, when needed, by a separate mechanical commit for imports, module declarations, and guard retargeting. No commit mixes a move with a semantic change.

## Vault Gaps Worth Capturing

1. Region-bounded source guards lose coverage on moves. [[hub moves must extend source scanning guard file lists]] covers fixed file lists but not guards that carve a region between two anchor symbols. A move of a function that sits between the anchors leaves the region green and blind, and retargeting the end anchor is not the fix. Capture the positive-anchor rule.
2. Positive and negative source guards fail differently during a move. A `contains` guard breaks loudly; a `!contains` guard goes silently blind. Capture that asymmetry as the reason a move needs a guard census rather than a green suite.
3. Near-identical Botster transport adapters differ in four specific ways: wake permit storage, deferred-flush filtering, close-from-host on `close_all`, and the test-only pressure setter. Capture that list so a later dedupe ticket does not converge them by accident.
4. The concrete cause of Hub scanner skip-mode leaks is brace counting inside string literals. A source-guard test needle such as `"DaemonRequest::ShutdownSession { session_id } => {"` contributes a net open brace, so the scanner never leaves skip mode. [[a source scanner can stay in cfg test skip mode through end of file]] records the failure mode but not this cause or the concatenation workaround. Capture both.
