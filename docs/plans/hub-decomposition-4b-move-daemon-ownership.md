# Hub Decomposition 4b: Move Daemon Ownership And Retire daemon_transport.rs

## Revision History

- Revision 1 (`4a82e36`, `411d1fb`): first plan.
- Revision 2 (this revision): answers Plan Review `review_1788031777_526782`, which returned `changes_required` with four findings.
  - `finding_1788031777_194351` (high, product): the late-message matrix omitted ownership-creating WebRTC requests. Fixed. The matrix now carries rows for `IssueLocalWebrtcBootstrap` (grant creation), `LocalWebrtcSignal` (peer, peer state, channel task, and peer-map entry creation), and the encrypted Hello arriving as `RegisterWebrtcAdmission` with its fail-closed insert gate, plus a row for the universal grant-tagged `Request` gate. A new `layered_gate_policy` answer records that four distinct liveness gates exist and must stay four, and acceptance checks 4a and 4b prove each gate separately.
  - `finding_1788031777_829830` (medium, process): mandatory Botster architecture context was not recorded. Fixed. [[botster-architecture]] and [[cli-patterns]] are now loaded and cited, the remaining "Must Load" entries are recorded as read and not implicated with a reason, and the Botster layer and test harness are named.
  - `finding_1788031777_626850` (medium, product): the removed public module path lacked direct downstream consumer proof. Fixed. Acceptance check 28 now replaces the zero-cost claim with four measured sub-checks; the measurement was run at Plan time and found no workspace member and no sibling repository importing the `botster_hub` library.
  - `finding_1788031777_230967` (low, process): step-completion evidence was empty. Fixed by passing evidence on the advance call for this visit.
- Revision 3 (this revision): answers Plan Review `review_1788032298_735771`, which returned `changes_required` with one high finding.
  - `finding_1788032298_630499` (high, product): revision 2's WebRTC grant lifecycle statements did not match production code. Fixed after reading `src/admission/grants.rs` and `src/transport/webrtc/peer.rs`. Revision 2 wrongly claimed that a grant "retires with its peer". It does not. `GrantRegistry::redeem` sets `redeemed = true` and **keeps** the row; peer removal never touches `grants`; the registry is shrunk only by `prune_expired_grants` at the next `issue_bootstrap`, and cleared wholesale only by `stop_all` at daemon shutdown. Revision 2 also used the informal word "spent" instead of the typed `RedeemedGrant` error and did not name the fixed five-error validation order. The matrix, `teardown_isolation`, and `ownership_identity` are corrected, and new acceptance check 4c forbids the move from introducing a grant removal on peer close.

## Target Repository And Target Id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- `run_id`: `run_1788030103_935368`. `ticket_id`: `ticket_1787894965_150479`.
- Base ref: `main` at `6b405b7`. The worktree branch starts at the same commit.
- The target repository comes from the ticket `target_id` through `list_spawn_targets`, not from the process working directory.

## Repository Playbook Loaded

- [[botster-hub-playbook]] -- the ownership charter for this repository.

## Other Role And Surface Playbooks And Atomic Notes Loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Mandatory Botster context required by the "Must Load" list in [[botster-planner-playbook]]:

- [[botster-architecture]] -- the current Botster domain map and source of architectural truth. It confirms that this ticket sits in the modular repository family, not the legacy monorepo, and its Core Architecture list already names six of the guard and extraction notes this plan applies.
- [[cli-patterns]] -- the Rust CLI, TUI, PTY, and terminal-layer index. It is a mixed-generation map, so it is used only to locate current notes; ownership comes from [[botster-hub-playbook]].
- [[spa-patterns]] -- read and found not implicated. This ticket changes no React, Catalyst, or entity-store surface.
- [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]] -- read and found not implicated. No Project Pipelines plugin path is in scope.
- [[botster orchestration should spawn agents with explicit target ids]] and [[botster orchestration prompts must bind agents to explicit worktrees]] -- applied. This run binds to `tgt_7e208a0c76a44980a83b63af976b1f22` and to this worktree, and the plan never treats the ambient directory as the target.
- [[current botster is a modular repository family not the legacy trybotster monorepo]] and [[legacy trybotster notes are not current modular botster contracts]] -- the scope gate. Every note cited in this plan was verified against the current `botster-hub` tree at `6b405b7`, not inherited from monorepo history.

Botster layer touched: the Rust Hub daemon control plane and its owner thread. No Lua core, plugin, TUI, SPA, MCP, or Rails layer changes. Test harnesses needed: Rust unit tests inside the moved modules, and the existing Rust integration lanes under `tests/hub_daemon_lifecycle/`. No headless runtime, browser, plugin fixture, or Rails harness is required.

Atomic notes that constrain this ticket:

- [[daemon transport extraction moves ownership before deleting the facade]] -- names this exact slice and the target directory map.
- [[Hub extraction must reduce ownership rather than only split files]] -- file count is not ownership proof.
- [[hub moves must extend source scanning guard file lists]] -- the two guard families and their ablation rule.
- [[fixed source guard lists need one ablation per added file]] -- one arm per added entry, or convert the list to a recursive walk.
- [[code moves need paired absence and presence source guards]] -- each moved symbol needs an absence arm and a presence arm.
- [[region bounded source guards need a required symbol anchor]] -- every bounded scan asserts its subject stays inside the region.
- [[a source scanner can stay in cfg test skip mode through end of file]] -- seed forbidden text after the final skipped block.
- [[exact Rust test ablations require a one test baseline]] -- exact filters must show one baseline test first.
- [[source guard ablations must not overlap a running full suite]] -- finish and restore every mutation before `./test.sh --locked`.
- [[botster runtime teardown lenses]] -- runtime-teardown class, answered in full below.
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]] -- the sibling policy that must survive the move.
- [[webrtc peer cleanup removes every per peer owner together]] -- the peer ownership sweep that moves with `LocalWebrtcPeerClosed`.
- [[PeerClosed attach occupancy must use the live attach route set]] -- occupancy rule inside the moved peer-closed handler.
- [[Owner loop must not stack maintenance and pump ahead of queued control]] -- the scheduling contract the owner loop must keep.
- [[Hub owner loop wakes only for mutations and pending resync]] -- the wake contract the owner loop must keep.
- [[Hub owner loop calls bounded Core lifecycle page APIs]] -- the bounded observation contract the owner loop must keep.
- [[Hub background fairness must stay policy-neutral]] -- one bounded scheduler for Pump and Maintenance.
- [[botster Hub Rust stays a trusted host kernel]] -- the boundary the new modules must preserve.
- [[botster data plane bypasses the hub through session and client actors]] -- the control plane must not absorb byte streams.
- [[Hub official gates must not set CARGO TARGET DIR]] -- the official locked gate uses the default target layout.
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]] -- select `1.97.0` in the pipeline shell.
- [[strict clippy can hide later crate diagnostics behind the first compile failure]] -- rerun the full Clippy gate after each repair.
- [[a ui contract import line change costs one test line in each generic client]] -- the downstream cost rule that yields a zero-cost claim here.
- [[colon worktree paths break cargo dyld library paths]] -- checked; this worktree path has no colon.

[[project-pipelines-playbook]] is not loaded. This ticket touches no Project Pipelines package or plugin path and no workflow policy.

## Context Loaded

Project architecture capture read in full, as the project checklist `checklist_1787894551_481961` requires: `ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md` (vault commit `8ef01f56`). The frozen target Hub directory map at lines 98 to 144 names `src/daemon/{owner_loop.rs,control/,shutdown.rs,error.rs}`. The module partition in this plan matches that map exactly. Line 148 states that `daemon_transport.rs` is a migration source, not a permanent facade, and step 6 at line 175 removes it after its last responsibility moves. Line 16 records that `daemon_transport.rs` still drives terminal work through `run_pump_observe_phase`, `observe_lifecycle_slice`, and `drain_runtime_once`; this ticket moves that code without changing it, and project step 6 removes the terminal pumping later.

Repository state read at `6b405b7`:

- `src/daemon_transport.rs`, 5441 lines. Current owner of the owner loop, the control-message dispatcher, the control-request dispatcher, the runtime control-request dispatcher, and a large family of per-request response helpers.
- `src/daemon.rs` (module root), `src/daemon/error.rs`, `src/daemon/shutdown.rs`. The `daemon` module already exists and already owns errors and shutdown classification.
- `src/daemon_package_control.rs`, 621 lines, declared from inside `daemon_transport.rs` with `#[path = "daemon_package_control.rs"] mod daemon_package_control;` and reached with `pub(super)` visibility.
- `src/lib.rs`: `pub mod daemon_transport;`, the crate-root `pub use daemon_transport::{...}` DTO block including `request as daemon_transport_request`, `serve_daemon`, and `stream_attach`; the `architecture_summary()` crate-export census; and the `production_sources_reject_terminal_drain_and_snapshot_phase_decode` source guard.
- In-crate importers of `crate::daemon_transport::*`: `src/transport/unix/{listener.rs,connection.rs,mux_write.rs}`, `src/transport/webrtc/{peer.rs,signaling.rs,control_channel.rs,subscription_channel.rs,delivery.rs,test_support.rs}`, `src/subscription/{entity.rs,attach_routes.rs,closed_events.rs}`, `src/daemon/shutdown.rs`.
- Crate-root importers of the flat re-export `daemon_transport_request`: `src/main.rs`, `src/mcp.rs`, `src/update.rs`, `src/local_runtime_process.rs`, `src/local_webrtc_smoke.rs`, and roughly 140 call sites in `tests/`.
- `docs/client-protocol.md` names `src/daemon_transport.rs` as an authoritative protocol source at line 6, and describes the production route at lines 1021, 1087, and 1207.
- CI: `.github/workflows/ci.yml` runs `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `./test.sh --locked` under Rust `1.97.0` and Zig `0.16.0`.
- Prior art: `15b35e3` ("Split WebRTC by channel role and retire local_webrtc.rs") is the 4a move-only commit and the template for this slice.

Guard inventory measured at base with `grep -rn "include_str!" src` and `grep -rn "hub_source(" tests`:

| Guard | File | Scans `daemon_transport.rs`? | Action |
|---|---|---|---|
| `production_sources_reject_terminal_drain_and_snapshot_phase_decode` | `src/lib.rs:1007` | yes, `src/lib.rs:1013` | replace the entry with every new `src/daemon/**` file |
| `no_lua_dispatch_in_terminal_input_or_output` | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:547` | yes, fixed list, plus a recursive `src/` walk | replace the entry with every new `src/daemon/**` file |
| `terminal_input_travels_as_a_json_control_request` | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:444` | yes | retarget to `src/daemon/control/sessions.rs`, needles byte-identical |
| `owner_loop_and_projection_sources_reject_unbounded_and_product_policy` | `tests/session_projection_owner_loop.rs:169` | yes, with a needle exemption at `:191` | replace the entry with every new `src/daemon/**` file and re-derive the exemption per destination |
| `unix_listener_connection_and_mux_left_daemon_transport` | `src/daemon_transport.rs:4182` | it is a self-scan of the deleted file | keep the name, move it with the owner loop, retarget the absence half to every `src/daemon/**` file |
| `source_stays_control_plane` | `src/daemon_maintenance.rs:1946` | no, self-scoped | unchanged |
| `host_control_fair_write` scans | `src/host_control_fair_write.rs:158` | no | unchanged |
| `subscription_ownership_baseline` WebRTC, attach, closed-event, and Cargo scans | same file | no | unchanged |

## Scope And Non-Scope

### In scope

1. Move the Hub owner loop into `src/daemon/owner_loop.rs`.
2. Split `handle_control_message`, `handle_control_request`, and `handle_runtime_control_request` by request family under `src/daemon/control/`.
3. Move `src/daemon_package_control.rs` under the package control family and drop the `#[path]` declaration hack that only existed to host it inside `daemon_transport.rs`.
4. Repair every import in `src/lib.rs`, the subscription modules, the transport modules, and `src/daemon/shutdown.rs` so each one names the new owner directly. No forwarding facade, no re-export shim module.
5. Update `docs/client-protocol.md` so it names the new authoritative sources.
6. Delete `src/daemon_transport.rs`.
7. Move the unit tests in `src/daemon_transport.rs` into the modules that own their subjects, with leaf test names unchanged.
8. Extend both source-guard families to the new files, with the ablation coverage the charter requires.

### Explicitly out of scope

- The wake-driven data plane, dedicated subscription DataChannels, and `src/data_plane/driver.rs`. Those are project steps 5 through 7.
- Any Core pin change. `Cargo.toml` and `Cargo.lock` `botster-core` revisions stay exactly as they are.
- Any DTO shape, serde name, protocol version, error code, scheduling budget, or runtime behavior change.
- Renaming the public crate-root name `daemon_transport_request`. It stays exactly as it is. A rename would churn about 140 test call sites and five production files for no product gain. It is registered as a follow-up candidate below.
- Splitting `src/daemon_maintenance.rs`, `src/runtime.rs`, `src/packages.rs`, or `src/main.rs`.
- Rewriting historical documents under `docs/plans/`. They record what was true when they were written.
- Converting the fixed `include_str!` guard list into a recursive walk. That is a guard-semantics change, and it would newly scan files that were never scanned. It is registered as a follow-up candidate below.

## Repository Ownership Boundaries And Cross-Repository Dependencies

- Everything in this ticket is inside `botster-hub`. No file outside `botster-hub` changes.
- Cross-repository dependencies: none to register. The only ordering dependency, `ticket_1787894421_128594` (Hub decomposition 4a), is closed and merged; `src/local_webrtc.rs` no longer exists and `src/transport/webrtc/` is in place.
- Core boundary: unchanged. Hub keeps calling bounded Core lifecycle page APIs from the owner loop, and Hub still does not schedule terminal bytes.
- Client boundary: unchanged. `botster-hub-client` owns the wire DTOs. Hub re-exports them from the crate root, and after this move the re-export is sourced directly from `botster_hub_client` and from `crate::daemon::error` rather than through `daemon_transport`.
- Public surface delta: the module path `botster_hub::daemon_transport::*` disappears. Every symbol it re-exported keeps its crate-root path. `pub mod daemon_transport` is removed from the crate-root module census in `src/lib.rs`, exactly as `local_webrtc` was removed in `15b35e3`.

## Assumptions And Unknowns

1. **Assumption.** Removing the `pub mod daemon_transport` module path is acceptable because the ticket requires the file to stop existing with no forwarding facade, and every symbol keeps its crate-root path. Measured at Plan time and recorded as acceptance check 28: no workspace member and no sibling repository imports the `botster_hub` library, so the module path has no external consumer to break. If Plan Review reads "public DTOs remain unchanged" as also freezing the module path, the ticket is self-contradictory and the Implementer must stop and ask.
2. **Assumption.** The crate-root export census names `"daemon_transport Daemon* DTO re-exports"` and `"serve_daemon / daemon_transport_request"`. Those census strings stay byte-identical, because `architecture_summary_keeps_client_contract_reexports_public` looks them up by literal name and `daemon_transport_request` remains the real exported name. Only the `"daemon_transport"` module row is removed.
3. **Assumption.** `pub(super)` items reached from `daemon_package_control` become `pub(crate)` or `pub(in crate::daemon::control)` after the move. This is a mechanical visibility re-scope forced by the new parent, not a surface change; every one of them is crate-private today and stays crate-private.
4. **Assumption.** The base is green. `main` is at `6b405b7`, which is after the 4a verification report and after the `webrtc_terminal_output_is_byte_exact` flake repair merged at `ddb2de9`. The Implementer must confirm with a baseline `./test.sh --locked` before starting, because acceptance check 12 is an absolute full-suite gate with no pre-existing-failure excuse.
5. **Unknown, Implementer resolves.** Which destination files inherit the product-policy needles (`botster-terminal-protocol-client`, `ProcessExited`, `botster-workspaces`, `membership`) that `tests/session_projection_owner_loop.rs` currently exempts for `src/daemon_transport.rs` as a whole. The Implementer must measure this per destination file and exempt only the files that actually contain a needle. If `src/daemon/owner_loop.rs` contains none, it enters the guard unexempted, which strengthens the guard at zero behavioral risk.
6. **Unknown, Implementer resolves.** Whether any request family is small enough that a separate module is noise. The partition below is the required default. The Implementer may merge two adjacent families only by recording the merge and its reason in the implementation report, and never by leaving a variant without exactly one module owner.

## Affected Surfaces And Files

### New files and their owned responsibilities

`src/daemon/owner_loop.rs` -- the Hub owner thread. Owns `serve_daemon`, `OwnerEvent`, `OwnerPollDecision`, `classify_owner_poll`, `receive_owner_event`, `retry_client_event_cleanups`, `owner_maintenance_pending`, `mark_due_reconciliation`, `run_one_owner_background_slice`, `run_one_owner_maintenance_slice`, `run_one_pump_phase`, `run_inventory_reconcile_phase`, `run_pump_observe_phase`, `DaemonControlState`, `PendingRuntimeState`, `DaemonEgressDiagnostics`, `egress_backpressure_diagnostic`, `record_egress_write_failure`, `send_control_response`, `wait_for_response_delivery`, `install_signal_forwarder`, `tick`, `should_mark_pump_after_control`, `request_succeeded`, and `ENTITY_RECONCILIATION_INTERVAL`.

`src/daemon/control.rs` -- the control module root and the three dispatchers only. Owns `handle_control_message`, `handle_control_request`, `handle_runtime_control_request`, `DaemonObservability`, `runtime_client_id`, `control_request_operation_label`, `request_id`, and the shared small response helpers `events_response`, `attach_bind_operator_error`, and `missing_session_drain_error`. After the split each dispatcher arm calls one family module and holds no per-request business logic.

`src/daemon/control/message.rs` -- the control vocabulary: `ControlMessage`, `ControlSender`, `ControlReplySender`, `DaemonDeliveryKind`, `daemon_delivery_kind`, `EgressWriteClass`, `egress_write_class`. Transport modules import from here.

`src/daemon/control/connection.rs` -- `AcceptedConnection`, `RejectedConnection`, `RegisterUnixAdmission`, and `RegisterWebrtcAdmission` handling.

`src/daemon/control/sessions.rs` -- `Status`, `ListSessions`, `RemoveSession`, `Spawn`, `Attach`, `Detach`, `SendInput`, `ModeGatedInput`, `Resize`, `ShutdownSession`, `Drain`, `ReadScreen`, `ReadModeFlags`, `CaptureSnapshot`.

`src/daemon/control/session_types.rs` -- the session-type request family, `session_type_entity_snapshot`, `session_type_definition_map`, `advance_session_type_generation_if_changed`, `force_advance_session_type_generation`, `is_invalid_repo_session_types_error`, `ensure_repo_session_types_valid_for_enabled_root`, `ensure_update_would_not_enable_invalid_repo_session_types`.

`src/daemon/control/spawn_targets.rs` -- spawn-target and worktree requests, `persist_spawn_targets`, `persist_spawn_targets_with_worktrees`, `persist_worktrees`, `daemon_targets`, `emit_worktree_lifecycle_event`.

`src/daemon/control/packages.rs` -- package, app, route, navigation, and entrypoint requests, plus `list_packages_response`, `show_package_response`, `package_decision_response`, `supervised_launch_contract`, `apply_entrypoint_snapshots`, `runtime_path`, `package_update_status`, `package_update_plan`.

`src/daemon/control/packages/mutations.rs` -- the verbatim content of `src/daemon_package_control.rs`, declared as a plain `mod` by `packages.rs`.

`src/daemon/control/messaging.rs` -- `Whoami`, `PostMessage`, `ReceiveMessages`, `AckMessage`, `NotifySession`, and `MESSAGE_CONTENT_TYPE`.

`src/daemon/control/plugins.rs` -- `PluginMcpListTools`, `PluginMcpCallTool`, `PluginSurfaceRender`, `PluginSurfaceAction`, `PluginLifecycleStatus`.

`src/daemon/control/entities.rs` -- `SubscribeEntities` and `UnsubscribeEntities`, on both the control-message and control-request paths.

`src/daemon/control/events.rs` -- `SubscribeEvents`, `UnsubscribeEvents`, `handle_client_event_request`, and `events_from_client`. `events_from_client` is the only `HubClientEvent` to `DaemonEvent` mapper and is the exact site the `DaemonEvent::*` forbidden-construct entries protect, so this file is a mandatory guard-list entry.

`src/daemon/control/webrtc.rs` -- `IssueLocalWebrtcBootstrap`, `LocalWebrtcSignal`, `issue_local_webrtc_bootstrap_response`, `local_webrtc_peer_gone_request_error`, `LocalWebrtcPeerClosed` handling, `detach_local_webrtc_subscriptions`, `persist_local_webrtc_terminal_record`.

`src/daemon/control/host.rs` -- `DaemonShutdown`, `CheckHubUpdate`, and `HubUpdateCheckCompleted`.

### Deleted files

- `src/daemon_transport.rs`.
- `src/daemon_package_control.rs`, whose content moves to `src/daemon/control/packages/mutations.rs`.

### Changed files

- `src/daemon.rs` -- declare `pub mod owner_loop;` and `pub mod control;`.
- `src/lib.rs` -- remove `pub mod daemon_transport;`; re-source the crate-root `pub use` block from `botster_hub_client`, `crate::daemon::error`, `crate::daemon::owner_loop::serve_daemon`, and `crate::transport::unix::connection::{DaemonConnection, request, stream_attach}`; remove the `daemon_transport` census row and the crate-root module-list entry; retarget the source-guard list; update the module-doc example text at `src/lib.rs:33` only if it names a path that no longer exists.
- `src/transport/unix/{listener.rs,connection.rs,mux_write.rs}` -- import `ControlMessage` and friends from `crate::daemon::control::message`, and `daemon_response_base` from its real owner `crate::client_api_dto::response`.
- `src/transport/webrtc/{peer.rs,signaling.rs,control_channel.rs,subscription_channel.rs,delivery.rs,test_support.rs}` -- same import repair, plus `handle_control_message` from `crate::daemon::control` and `response_records_attach_ownership` from its real owner `crate::subscription::attach_routes`.
- `src/subscription/{entity.rs,attach_routes.rs,closed_events.rs}` -- import `DaemonControlState` and `PendingRuntimeState` from `crate::daemon::owner_loop`, and `session_type_entity_snapshot` from `crate::daemon::control::session_types`.
- `src/daemon/shutdown.rs` -- import `tick` from `crate::daemon::owner_loop`.
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` and `tests/session_projection_owner_loop.rs` -- guard path retargeting only.
- `docs/client-protocol.md` -- replace `src/daemon_transport.rs` at line 6 with the new authoritative sources, and update the route prose at lines 1021, 1087, and 1207 to name `src/daemon/owner_loop.rs` and `src/daemon/control/`.

## Risks

1. **Guards stay green while losing their subject.** The exact failure mode of [[hub moves must extend source scanning guard file lists]], and the highest-probability failure here because five guards name `src/daemon_transport.rs`. Mitigated by acceptance checks 15 through 20.
2. **A region-bounded or self-scanning test loses meaning when its file disappears.** `unix_listener_connection_and_mux_left_daemon_transport` scans the deleted file. Mitigated by check 19, which keeps the name and gives the absence half a live subject.
3. **The `session_projection_owner_loop` needle exemption silently widens.** Exempting every new file the way `daemon_transport.rs` was exempted would weaken the guard across fourteen files. Mitigated by check 20, which requires a per-destination needle measurement.
4. **`pub(super)` re-scoping accidentally widens visibility.** Mitigated by check 9, which forbids any new `pub` item and requires each re-scoped item to stay crate-private.
5. **The dispatcher split changes match-arm order or fall-through.** `handle_runtime_control_request` ends in an `unreachable!("package requests are handled before runtime borrow")` arm that depends on the outer dispatcher having already consumed the package family. Splitting the dispatchers can move a variant across that boundary and turn the `unreachable!` into a live panic. Mitigated by check 6 and by the full suite.
6. **A borrow-checker repair becomes a behavior change.** The runtime handler exists as one function partly because of the `&mut HubDaemon` borrow. Splitting it can tempt a clone, an extra lock, or a reordered borrow. Mitigated by check 8, which forbids any new clone, lock, or allocation on a control path in this commit; if one is unavoidable, the Implementer stops and re-plans.
7. **The suite is long and the host is shared.** A source mutation made for an ablation while the full suite runs invalidates that run, per [[source guard ablations must not overlap a running full suite]]. Mitigated by checks 14 and 21.
8. **Scale.** The move is about 5400 lines across roughly fifteen new files with about 140 test call sites depending on the surviving crate-root name. The one-commit rule keeps review tractable only if the commit is a pure move; check 22 enforces the separation.

## Runtime-Teardown Class

`teardown_class_applies`: **yes**. The move relocates the `ControlMessage::LocalWebrtcPeerClosed` handler, `detach_local_webrtc_subscriptions`, `persist_local_webrtc_terminal_record`, `local_webrtc_peer_gone_request_error`, the `ShutdownSession` control arm, the owner-loop pump and observe phases, and the daemon shutdown response-delivery wait. Those are peer lifecycle, session teardown, and control-plane close paths. [[botster runtime teardown lenses]] applies whenever that code is touched, including a move.

This ticket changes no teardown behavior. Each answer below states the behavior that must survive unchanged and names the oracle that proves it survived.

`teardown_isolation`: One failed peer's ownership set is its attached subscriptions, its entity subscriptions, its connection-scoped event holders, and its terminal record. The grant row is **not** in that set; it stays in `GrantRegistry` as a redeemed, expiring record. `LocalWebrtcPeerClosed` carries the peer-owned snapshot from `peer.rs` into the control plane, which detaches the attached subscriptions, retains only rows still owned by a removed grant, and calls `cleanup_connection` for the event holders. On ordinary close, siblings keep working. On ultimate close failure, Hub sacrifices every peer on the dedicated runtime per [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]. After the move, `src/daemon/control/webrtc.rs` is the single control-plane owner of that sweep, and `src/transport/webrtc/peer.rs` remains the transport-side owner. Oracles preserved by name: `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`, `client_eof_detaches_connection_subscriptions`, `connection_cleanup_ignores_only_an_already_removed_session`, `attach_operator_error_does_not_detach_on_client_eof`.

`teardown_bounds`: The bounds are unchanged and none of them moves out of its current owner. `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and `LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE` stay in `src/transport/webrtc/peer.rs`. `DAEMON_CLIENT_WRITE_TIMEOUT` stays in `src/admission/budgets.rs`. `MAX_OWNER_TURN_MS` and `MAX_READY_OPERATION_WAIT_MS` stay in `src/daemon_maintenance.rs`. The daemon-stop path waits for response delivery through `wait_for_response_delivery`, which releases on delivery, on owner drop, on receiver drop, and on write failure; all four legs move together into `src/daemon/owner_loop.rs`. A hang in a close path still takes the fail-closed route rather than blocking the owner thread. Oracles preserved by name: `daemon_shutdown_waits_for_response_delivery_before_stopping`, `daemon_shutdown_releases_when_delivery_owner_drops`, `daemon_shutdown_releases_when_response_receiver_drops`, `daemon_shutdown_write_failure_releases_stop_and_preserves_error`, `hanging_data_channel_local_close_still_runs_cleanup_once_within_bound`.

`late_message_matrix`: no row changes. Each row names its control-plane owner file after the move.

| Message type | Creates durable ownership | Control-plane owner after move | Owner tag | Rejection after terminal failure | Residual sweep on PeerClosed race |
|---|---|---|---|---|---|
| `IssueLocalWebrtcBootstrap` | **yes.** It issues the grant that every later WebRTC ownership row is keyed on | `control/webrtc.rs` dispatch; the registry stays in `src/admission/grants.rs`, which this ticket does not move | the grant binds `(grant_id, grant_secret, expected_origin)` with `expires_at = now + GRANT_TTL_SECONDS` (120) and `redeemed: false`; the origin must equal the running entrypoint's `local_url` origin | six typed `local_webrtc_bootstrap_*` legs reject unsupported entrypoint, uninstalled package, disabled package, missing entrypoint, non-running entrypoint, and origin mismatch; no grant is issued on any failing leg, per [[webrtc bootstrap origin must be requested after the package server binds]] | **the grant row is never swept by `PeerClosed`.** Peer removal touches `peers`, `peer_states`, and `stale_close_peers` only. The row is marked `redeemed = true` at signal time and stays in `GrantRegistry` until the next `issue_bootstrap` call prunes it past its 120-second expiry, or `stop_all` clears the registry at daemon shutdown. Retention is the replay defense: the `redeemed` flag is what makes a grant single-use, so removing the row on peer close would make the grant id reusable |
| `LocalWebrtcSignal` | **yes, and it is the largest ownership creation in this file.** It redeems the grant and creates the live `RTCPeerConnection`, its peer state, its data-channel task, and its entries in the peer maps | `control/webrtc.rs` dispatch; mechanics stay in `src/transport/webrtc/{signaling.rs,peer.rs}` and `src/admission/grants.rs`, none of which this ticket moves | `GrantRegistry::redeem` validates `(grant_id, grant_secret, origin)` before any peer exists | five typed grant errors in a fixed order: `MissingGrant`, then `RedeemedGrant`, `ExpiredGrant`, `SecretMismatch`, `OriginMismatch`. A grant is single-use, so a replayed signal after peer close fails on `RedeemedGrant` and creates no second peer | the peer created here **is** the ownership set that `LocalWebrtcPeerClosed` later sweeps; a signal that fails creates no peer to sweep, and the grant row is unaffected either way |
| Encrypted WebRTC Hello, delivered as `RegisterWebrtcAdmission` | **yes.** It creates the terminal admission entry and the host compatibility record | `control/connection.rs` | `grant_id` | **fail-closed insert gate.** The admission and the compatibility record are inserted only inside `if daemon.local_webrtc().has_live_peer(&grant_id)`. A Hello for a peer that is no longer live is dropped, not inserted. This gate must survive the move verbatim | nothing to sweep, because nothing was inserted; a live-peer admission retires in the removed-grant loop |
| `RegisterUnixAdmission` | **yes** | `control/connection.rs` | `client_id` | the Unix path has no peer-liveness gate; the connection itself is the liveness proof | the admission entry retires on Unix EOF cleanup |
| Any grant-tagged `Request` | gate, not a creator | `control.rs` dispatcher, before family delegation | `grant_id` carried on `ControlMessage::Request` | **universal gate.** `has_live_peer` is checked before dispatch, and a dead peer returns `local_webrtc_peer_gone_request_error(operation)` with the typed operation label. This gate protects every ownership-creating request that arrives over WebRTC, including `Attach`, `Spawn`, `SubscribeEvents`, and `ShutdownSession` | the gate prevents the race rather than sweeping it |
| `Attach` | yes | `control/sessions.rs`; sweep in `control/webrtc.rs` | `(session_id, subscription_id)` plus `attach_owner_grant_ids` keyed by `grant_id` | covered by the universal grant-tagged gate above; `response_records_attach_ownership` records nothing for `OperatorError` or `attach_failed` | `detach_local_webrtc_subscriptions` sweeps the `LocalWebrtcPeerClosed` snapshot; `attach_owner_grant_ids.retain` drops only rows owned by a removed grant |
| `Detach` | retires ownership | `control/sessions.rs` | same identity | a failed `Detach` leaves the row for the sweep | idempotent against an already-removed row |
| `SubscribeEntities` | yes | `control/entities.rs`; sweep in `control/webrtc.rs` | `subscription_id` plus `owner_grant_id` | **its own `has_live_peer` gate**, separate from the `Request` gate, replying with a typed `local_webrtc_peer_gone` entity-subscription error; a failed subscribe records nothing | a snapshot id is removed only when the current row is unowned or still owned by a removed grant |
| `UnsubscribeEntities` | retires ownership | `control/entities.rs` | same identity | **its own owner-checked gate.** After peer loss it removes a row only when that row is unowned or still owned by the gone grant, and it still replies `EntityUnsubscribed` idempotently | a late unsubscribe is a no-op against a removed row |
| `SubscribeEvents` | yes | `control/events.rs`; holder in `src/subscription/package_events.rs` | connection-scoped holder keyed by `grant_id` or Unix client id | exact `(owner, name)` admission; a rejected contract creates no holder | `cleanup_connection` runs inside the removed-grant loop; admitted jobs survive to Core completion by design |
| `UnsubscribeEvents` | retires ownership | `control/events.rs` | same holder identity | a late unsubscribe cannot retire another peer's holder | `cleanup_connection` is idempotent |
| `Spawn` | yes, and intentionally peer-independent | `control/sessions.rs` | `session_id`; the worker is Hub-owned and durable | a failed spawn creates no session | **not swept by PeerClosed.** Only `ShutdownSession` or Hub stop retires it. Restated so the move does not silently acquire a session sweep |
| `ShutdownSession` | retires ownership | `control/sessions.rs`, classification in `src/daemon/shutdown.rs` | exact `(session_id, subscription_id, generation)` suppression before Core teardown | typed `Found`, `Absent`, and `Err` results preserved | suppression precedes Core shutdown, per [[ShutdownSession suppresses exact route generations before Core teardown]] |
| Unix client EOF | retires ownership | `control/connection.rs` | `client_id` | replies finish before EOF cleanup | `client_eof_detaches_connection_subscriptions` covers the sweep |
| Terminal and entity frames | no | not control plane; `transport/webrtc/{control_channel.rs,delivery.rs}` | subscription id on the frame | prior-generation frames rejected | the channel closes with the peer |

`production_path_proof`: The production path is unchanged. For peer creation it is browser page load, then `IssueLocalWebrtcBootstrap` producing an origin-bound grant, then `LocalWebrtcSignal` producing the peer and its answer, then the encrypted Hello arriving as `RegisterWebrtcAdmission`. After the move the first three control-plane legs live in `src/daemon/control/webrtc.rs` and the fourth in `src/daemon/control/connection.rs`. For peer loss it is peer connection state change, then `LocalWebrtcHandler::on_connection_state_change`, then `cleanup_once`, then `remove_peer`, then `ControlMessage::LocalWebrtcPeerClosed` on the owner thread, then `detach_local_webrtc_subscriptions`, then `park_runtime_if_idle`. After the move the owner-thread legs live in `src/daemon/owner_loop.rs` and `src/daemon/control/webrtc.rs`. For a client request it is Unix socket or WebRTC control channel, then `ControlMessage::Request`, then `handle_control_message`, then `handle_control_request`, then `handle_runtime_control_request`, then `HubClientApi::handle_request`, then `HubRuntime`. The live oracles are the existing `tests/hub_daemon_lifecycle/` lanes, which drive real peers and a real daemon child through the production handlers. This ticket adds no new live proof; it must show the same lanes still pass and still reach the production dispatcher. Terminal record files alone remain insufficient, per [[terminal webrtc failure records do not prove peer runtime teardown]].

`layered_gate_policy`: four distinct liveness gates exist today, and the split must keep four. They are the `RegisterWebrtcAdmission` insert gate, the `Request` pre-dispatch gate, the `SubscribeEntities` reply gate, and the `UnsubscribeEntities` owner-checked removal gate. Each has a different failure response, so a row that one gate already rejects proves nothing about the others. The Implementer must not collapse them into one shared helper during the split, and acceptance check 4a requires one red-on-revert arm per gate.

`ownership_identity`: `grant_id` identifies a WebRTC peer, and a grant is single-use. `GrantRegistry::redeem` sets `redeemed = true` and keeps the row, so a replayed `LocalWebrtcSignal` for a closed peer's grant fails on `RedeemedGrant` and can never produce a replacement peer under the same id. The grant map and the peer map are independent: `has_live_peer` reads `peers`, never `grants`. `grant_id` identifies a WebRTC peer. `client_id` identifies a Unix connection. `(session_id, subscription_id, generation)` identifies a terminal route. A delayed `LocalWebrtcPeerClosed` snapshot cannot delete a row now owned by a live replacement peer, because every removal is gated on the row still being unowned or owned by a removed grant. Both the identity fields and the gating logic move unchanged into `src/daemon/control/webrtc.rs`.

`sibling_fail_closed_policy`: On successful close, siblings keep working. On ultimate close failure, Hub sacrifices every peer on the dedicated runtime and sweeps all owners together, per [[webrtc peer cleanup removes every per peer owner together]]. The policy, its blast radius, and its oracle are unchanged, and the oracle lives in `src/transport/webrtc/peer.rs`, which this ticket does not move.

## Acceptance Checks And Tests

Ownership checks, which prove this is an extraction and not a file split:

1. `src/daemon_transport.rs` and `src/daemon_package_control.rs` do not exist. Prove with `git ls-files src | grep -E 'daemon_transport\.rs|daemon_package_control\.rs'` returning no output.
2. No forwarding facade exists. Prove that no file under `src/` re-exports an owner-loop or control symbol from outside its owner module, that `src/lib.rs` sources `serve_daemon` from `crate::daemon::owner_loop`, `DaemonConnection`, `request`, and `stream_attach` from `crate::transport::unix::connection`, `DaemonTransportError` and `DaemonTransportResult` from `crate::daemon::error`, and every `Daemon*` DTO from `botster_hub_client`.
3. Every `DaemonRequest` variant has exactly one module owner. Add a source guard that, for each family module listed in "Affected Surfaces And Files", asserts its variant names appear in that module and asserts the paired absence of those same variant names in every other control family module, per [[code moves need paired absence and presence source guards]]. The dispatcher file may name a variant only in a delegating arm.
4. Every `ControlMessage` variant has exactly one handler module. Same paired presence and absence form as check 3.
4a. The four WebRTC liveness gates survive as four distinct gates. Assert the presence of each `has_live_peer` call site in its named owner file: the insert gate in `control/connection.rs`, the pre-dispatch gate in `control.rs`, the reply gate in `control/entities.rs` for `SubscribeEntities`, and the owner-checked removal gate in `control/entities.rs` for `UnsubscribeEntities`. Assert that each keeps its own distinct failure response: the dropped insert, `local_webrtc_peer_gone_request_error(operation)`, the typed `local_webrtc_peer_gone` entity-subscription error, and the idempotent `EntityUnsubscribed` reply. Add one red-on-revert arm per gate, because a row that one gate already rejects proves nothing about the other three, so each gate needs its own red-on-revert arm per [[a regression test must be shown to go red with the fix reverted]] and by the same reasoning as [[fixed source guard lists need one ablation per added file]]. Collapsing the four into one shared helper fails this check.

4b. The `Request` gate still precedes family delegation. Assert by inspection and by a source guard that the `has_live_peer` check in `control.rs` appears before any call into a family module, so no ownership-creating request can reach `control/sessions.rs`, `control/entities.rs`, or `control/events.rs` behind a dead peer.

4c. The grant lifecycle is untouched. `src/admission/grants.rs` and `src/transport/webrtc/peer.rs` do not move in this ticket, so prove with `git diff` that both are unchanged except for import paths. Specifically assert that the move introduces no grant removal on peer close: `remove_peer`, `take_remove_result`, `fail_closed_drop_dedicated_runtime`, and `park_runtime_if_idle` still touch only `peers`, `peer_states`, and `stale_close_peers`, and `GrantRegistry` is still shrunk only by `prune_expired_grants` at the next `issue_bootstrap` and cleared only by `stop_all`. Removing a grant row on peer close would make a grant id reusable and defeat the `redeemed` replay defense, so a source guard asserts that no `src/daemon/**` file names `grants` removal. The existing oracles `grant_validation_runs_redeemed_expiry_secret_then_origin` and `issuing_bootstrap_prunes_expired_grants_and_keeps_live_replay_diagnostics` stay in `src/admission/grants.rs` with their names and bodies unchanged.

5. `src/daemon/control.rs` holds dispatch only. Assert that the three dispatcher functions contain no call into `HubClientApi`, `HubRuntime`, `PackageRegistry`, or persistence, and that each arm body is a single delegating call.
6. The `unreachable!("package requests are handled before runtime borrow")` invariant still holds. Prove the exact string survives, and prove by inspection that every variant named in that arm is consumed by `control/packages.rs` before the runtime borrow. Record the variant list in the implementation report.
7. `src/daemon/**` contains no transport mechanism symbol. Assert the absence of `accept_connections`, `handle_connection_async`, `MuxWriteState`, `read_async_frame`, `prepare_socket_path`, and `unix_event_flush_stalled` in every `src/daemon/**` file, with a red-on-revert arm.
8. No new clone, lock acquisition, allocation, or `await` point is introduced on any control path. Prove with `git diff --color-moved=dimmed-zebra` showing the dispatcher bodies as pure relocation, and by listing any line that is not moved verbatim with its reason.
9. No item gains wider visibility. Prove that every `pub(super)` item re-scoped by the move is `pub(crate)` or narrower afterward, and that no item becomes `pub`.

Behavior-preservation checks:

10. No protocol, DTO, serde name, error code, or scheduling-budget change. Prove with `git diff` over `src/client_api_dto/`, `src/daemon/error.rs`, `src/admission/budgets.rs`, and `src/daemon_maintenance.rs` showing zero functional change, and with zero `serde` attribute changes in the whole diff.
11. The Core pin is unchanged. Prove with `git diff` over `Cargo.toml` and `Cargo.lock` showing no `botster-core` revision change.
12. **Absolute full-suite gate.** `./test.sh --locked` passes at final HEAD on a quiet host with zero failures. The base `6b405b7` is green, so no failure may be attributed to a pre-existing condition. Any failure is either fixed inside move-only scope or reported as a stop. These lanes must appear in the run and pass: `subscription_ownership_baseline.rs`, `sessions.rs`, `shutdown.rs`, `packages.rs`, `cli.rs`, `event_plane_saturation.rs`, `webrtc_proofs.rs`, `webrtc_terminal_adapter.rs`, `session_projection_owner_loop.rs`, `hub_mcp_test.rs`.
    - 12a. Record the baseline full-suite result at `6b405b7` before the move, and the final result at HEAD. A green isolated rerun is not a substitute.
13. Exact proof-name preservation. Capture the test inventory at base and at final HEAD with `cargo test --workspace --locked -- --list`, and compare the multiset of leaf test names. The two multisets must be identical: no proof name removed, renamed, or reduced in count. Record a rename map from each old module path to its new module path, with the leaf name unchanged on every row. Any intentionally removed duplicate must be listed by name with its reason.
14. The `daemon_test_guard` serialization contract is unchanged. Every moved test that took the guard still takes it, and none of the new module boundaries introduces a second process-global latch, per [[process global test latches require daemon guard serialization]].

Guard checks:

15. Both guard families are enumerated before and after the move with `grep -rn "include_str!" src` and `grep -rn "hub_source(" tests`, and both enumerations are recorded in the implementation report. Every row of the "Context Loaded" guard table is accounted for with its post-move target or an explicit unchanged statement. A count alone does not satisfy this check.
16. Every new `src/daemon/**` file appears in the `src/lib.rs` `production_sources_reject_terminal_drain_and_snapshot_phase_decode` list, including `src/daemon/control.rs`, `src/daemon/control/message.rs`, and `src/daemon/control/packages/mutations.rs`. `src/daemon/control/events.rs` is mandatory, because it holds `events_from_client`, the only `HubClientEvent` to `DaemonEvent` mapper.
17. Each added `src/lib.rs` guard entry has its own red-on-revert ablation, per [[fixed source guard lists need one ablation per added file]]. One representative arm is not accepted. Add one scanner-liveness arm on a still-listed file. Every ablation uses a full-module-path `--exact` filter and shows a one-test baseline first, per [[cargo exact with a name prefix runs zero tests and exits zero]] and [[exact Rust test ablations require a one test baseline]].
18. The scanner final-state invariant covers every new file: each listed file must report `skip_open_at_eof == false`. For each new production file that ends with a `#[cfg(test)]` block, add a seeded-tail red arm that places a forbidden construct after that file's final skipped block, per [[a source scanner can stay in cfg test skip mode through end of file]]. One shared arm is not accepted.
19. `unix_listener_connection_and_mux_left_daemon_transport` keeps its exact leaf name and moves to `src/daemon/owner_loop.rs`. Its presence half is unchanged. Its absence half now scans every `src/daemon/**` file for the same seven needles, so the invariant keeps a live subject after the source file is deleted. Add a red-on-revert arm that seeds one needle into a `src/daemon/**` file.
20. `owner_loop_and_projection_sources_reject_unbounded_and_product_policy` lists every new `src/daemon/**` file. The Implementer measures, per destination file, which of the four product-policy needles it actually contains, and exempts only those files. Record the full matrix in the implementation report. Add a red-on-revert arm for the unbounded `observe_lifecycle(` assertion against `src/daemon/owner_loop.rs`.
21. Every guard ablation is completed and reverted before the official `./test.sh --locked` run starts, per [[source guard ablations must not overlap a running full suite]]. Confirm a clean `git status` before the gate.

Commit shape:

22. The move lands as **one compiling move-only commit**, as the ticket acceptance requires. That commit carries the file moves and only the changes the move itself forces: import paths, module declarations, `src/lib.rs` wiring, visibility re-scoping, guard path retargeting, and the `docs/client-protocol.md` source names. It must compile on its own and change no behavior. Review it with `git diff --color-moved=dimmed-zebra`. New assertions rather than retargeting, meaning the checks 3, 4, 5, 7, 19, and 20 guards, land in a separate follow-up commit so the move commit stays pure. No commit mixes a move with a semantic change.

Gate commands, run from this worktree with `RUSTUP_TOOLCHAIN=1.97.0` and with `CARGO_TARGET_DIR` unset, per [[Hub official gates must not set CARGO TARGET DIR]]:

23. `rustc --version` recorded from the same shell, showing `1.97.0`.
24. `cargo fmt --all -- --check`.
25. `cargo clippy --workspace --all-targets --locked -- -D warnings`, rerun in full after each repair, per [[strict clippy can hide later crate diagnostics behind the first compile failure]].
26. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` before the suite, per [[Hub suite runs prebuild the session worker before the locked test wrapper]].
27. `./test.sh --locked` on a quiet host. Any failure must be matched to its own named marker before anyone calls it unrelated.

Downstream proof required by the charter:

28. **Direct consumer proof for the removed module path, not a claim.** The one public-surface delta is the removal of `botster_hub::daemon_transport`. Measured at Plan time, no consumer of the `botster-hub` library crate exists outside this repository:
    - 28a. No workspace member depends on the root library. Prove with `grep -rn '^botster-hub *=' crates/*/Cargo.toml` returning no output. The five members are `botster-hub-client`, `botster-hub-installation`, `botster-hub-installer`, `botster-hub-test-support`, and `botster-ui-contract`; none names the root package as a dependency.
    - 28b. No sibling repository imports the library. Prove with `grep -rn 'botster_hub::' <repo>` over `botster-tui`, `botster-web`, `botster-workspaces`, `botster-project-pipelines`, `botster-tui-kit`, `restty`, and `botster-core`, returning no output in every repository. Every existing `botster-hub` string in those repositories names the **binary**, a build command, a package id, or a playbook slug, never the Rust library path. Record the per-repository command output in the implementation report.
    - 28c. No in-repository caller survives the removal. Prove with `grep -rn 'daemon_transport::' src tests` at final HEAD returning no output, while `grep -rn 'daemon_transport_request' src tests` still returns the preserved crate-root alias call sites.
    - 28d. The generic-client cost is therefore zero under [[a ui contract import line change costs one test line in each generic client]], because no wire DTO and no UI contract changes and every crate-root export keeps its name and type. This is now supported by 28a through 28c rather than asserted. Rebuilding `botster-tui` and `botster-web` is not required, because neither compiles against this library at all.
    - 28e. If any DTO, serde name, or exported type changes, or if 28a or 28b returns a hit at implementation time, the ticket has left move-only scope and the Implementer must stop and re-plan.

## Documentation

29. `docs/client-protocol.md` line 6 replaces `src/daemon_transport.rs` with `src/daemon/control/` and `src/daemon/owner_loop.rs`. The route prose at lines 1021, 1087, and 1207 names `serve_daemon` in `src/daemon/owner_loop.rs` and the dispatchers in `src/daemon/control/`. The protocol version, the DTO list, and the contract statements do not change. Historical documents under `docs/plans/` are not rewritten.

## Vault Gaps Worth Capturing

1. **A deleted file's self-scan needs a new subject, not deletion.** `unix_listener_connection_and_mux_left_daemon_transport` scans a file this ticket removes. The general rule, that an absence guard whose subject file disappears must be retargeted to the destination set rather than dropped, extends [[region bounded source guards need a required symbol anchor]] from a moved bound to a deleted subject file. Capture after check 19 lands.
2. **A whole-file guard exemption must be re-derived per destination when the file splits.** The `session_projection_owner_loop` exemption for `src/daemon_transport.rs` covered one file with mixed content. Splitting it into fourteen files makes a blanket exemption strictly weaker than the original. Capture after check 20 lands.
3. **A fixed `include_str!` list should become a recursive walk once its ablation cost exceeds its precision value.** This ticket adds about fourteen entries, each needing its own arm. The `no_lua_dispatch_in_terminal_input_or_output` guard already pairs a fixed list with a recursive walk and is the model. Capture as a follow-up ticket candidate against `botster-hub`.
4. **Layered liveness gates need one red arm per gate.** Hub has four distinct WebRTC `has_live_peer` gates with four different failure responses. A test row that an earlier gate already rejects never reaches the later gates, so a single ablation can leave three gates unproven. The vault has this reasoning for fixed source-guard lists but not for layered runtime gates. Capture after acceptance check 4a lands.
5. **A Hub bootstrap grant is retained after redemption on purpose.** `GrantRegistry::redeem` sets `redeemed = true` and keeps the row; peer close never removes it; the registry shrinks only through the lazy `prune_expired_grants` at the next `issue_bootstrap` and is cleared wholesale only by `stop_all`. Retention is the single-use replay defense, so a later cleanup that removes the row on peer close would silently make a grant id reusable. This is exactly the correct-looking cleanup a decomposition invites, and the vault does not record it. Capture after acceptance check 4c lands.
6. **A crate-root alias can outlive the module it was named after.** `daemon_transport_request` stays after `daemon_transport` is gone. Capture the rule that a move-only slice keeps the public alias and registers the rename separately, so a later reader does not read the stale name as a missed cleanup.
