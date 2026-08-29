# Implement report: Hub decomposition 3

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `list_spawn_targets` maps this id to `botster-hub` at `/Users/jasonconigliari/Projects/botster-hub` |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1787894419_699597` |
| Run | `run_1787990653_757857` |
| Step | `botster_stack_implement` (`run_step_1787992836_321469`) |
| Approved plan | `docs/plans/hub-decomposition-3-extract-shared-adapter-mechanics-and-unix-transport.md` revision 2 |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `a45cf7b936eb8e8af04a123dd349206d8ba52e41` |
| Product commit | `667648a91ea44394f07e8e0b038e332f6066fd26` |
| Runtime-teardown class | applies; every lens is preserved as a survive-the-move invariant |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

[[project-pipelines-playbook]] was not loaded. This ticket changes no Project Pipelines package, plugin, or workflow-policy path.

### Targeted atomic notes

- [[daemon transport extraction moves ownership before deleting the facade]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[hub moves must extend source scanning guard file lists]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[a known positive control proves a scan is live not that its pattern set is complete]]
- [[fixed source guard lists need one ablation per added file]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[proposed Core transport adapters use bounded writes without policy queues]]
- [[terminal adapters emit coalesced writable and closed wakes]]
- [[adapter accepted writes are not consumer flushed writes]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[Unix mux host frames flush before new terminal slots]]
- [[Unix mux host events are unsolicited control frames]]
- [[Fair host-control writing selects already-admitted frames]]
- [[Client event holders are connection-scoped]]
- [[botster runtime teardown lenses]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[a ui contract import line change costs one test line in each generic client]]
- [[test script required for rust tests not cargo test]]

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Keep Hub host-policy ownership.
- Shared transport holds no admission, route, grant, label, or product policy.
- Unix keeps framing, mux scheduling, and deferred-flush. WebRTC keeps permit wake and its mux.
- No item becomes `pub`. `pub(crate)` widening is recorded.
- Do not change the Core pin, DTO shapes, serde names, protocol version, or existing proof names.
- Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset.

## Files changed

| Path | Why |
| --- | --- |
| `src/transport.rs` | crate-private transport tree |
| `src/transport/shared.rs` | shared child modules plus ownership guards |
| `src/transport/shared/adapter_slot.rs` | one-slot write state shared by both adapters |
| `src/transport/shared/wake.rs` | permit wake and Unix notify-waiters sink |
| `src/transport/shared/close_reason.rs` | first-cause host vs Core close flags |
| `src/transport/shared/close_progress.rs` | `ClosedEventSliceProgress` and slice accumulator |
| `src/transport/unix.rs` | Unix child modules |
| `src/transport/unix/adapter.rs` | moved Unix adapter and connection mux |
| `src/transport/unix/listener.rs` | socket path, accept loop, rejection, client-id counter |
| `src/transport/unix/connection.rs` | accepted-connection driver, client `DaemonConnection`, cleanup |
| `src/transport/unix/mux_write.rs` | Unix framing, mux scheduling, async frame helpers |
| `src/unix_terminal_adapter.rs` | deleted after the move |
| `src/webrtc_terminal_adapter.rs` | rebuilt over the shared slot, wake, and close cause |
| `src/subscription/closed_events.rs` | consumes shared progress; needle repair; keeps route ownership |
| `src/daemon_transport.rs` | loses listener, connection, framing, mux; keeps control dispatch and `serve_daemon` |
| `src/lib.rs` | `pub(crate) mod transport`; `architecture_summary` row; scanner invariant; guard list |
| `src/admission/unix_hello.rs` | import path to Unix mux |
| `src/subscription/attach_routes.rs` | import path to Unix adapter types |
| `src/host_control_fair_write.rs` | Unix call-site scan retargeted to `mux_write.rs` |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | adapter path and transport file list |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | Unix adapter path |
| `docs/plans/hub-decomposition-3-extract-shared-adapter-mechanics-and-unix-transport.md` | plan (Plan visits) |
| `docs/reports/hub-decomposition-3-extract-shared-adapter-mechanics-and-unix-transport-implement.md` | this report |

Unchanged, as required: `src/main.rs`, `crates/botster-hub-client/`, `packages/hub-test-support/`, `Cargo.toml`, `Cargo.lock`, `src/client_api_dto/`, `src/daemon/error.rs`.

## Ownership boundaries preserved

- Hub still owns concrete Unix and WebRTC adapters. No transport crate. Core pin unchanged.
- Shared transport owns the one-slot write, wake sink, close-cause flags, and close-slice counters only.
- Subscription still owns `ClosedEventRoute`, `ClosedHandle`, route traversal, suppression, classification, and `DaemonEvent` construction.
- Unix still owns `UnixConnectionMux`, deferred flush, framing, and mux scheduling.
- WebRTC still owns `WebRtcConnectionMux`, permit wake, `close_events_admitted`, and `drop_pending_events`.
- `src/daemon_transport.rs` still owns `serve_daemon`, control dispatch, the owner loop, and `DaemonControlState`.
- Public paths `daemon_transport::DaemonConnection`, `request`, `stream_attach`, and `serve_daemon` remain. `transport` is `AlreadyInternal`.

## Cross-repo routing

None. No new ticket dependency. Downstream Web/TUI cost is zero because no public export, DTO field, serde name, or protocol version changed.

## Cut-line decisions

- `handle_entity_subscription_async` is connection role. It runs on the accepted-connection task and moved to `src/transport/unix/connection.rs`.
- `receive_control_response` moved with that connection driver.
- `handle_connection_cleanup`, connection task reaping, and `handle_connection` moved with the cleanup path.
- `handle_control_request`, `handle_control_message`, pump phases, and `*_response` helpers stayed in `src/daemon_transport.rs`.
- `read_async_frame` / `write_async_frame` moved with Unix framing.

## Visibility widening (`pub(crate)` only; none `pub`)

- `DaemonObservability` and its fields
- `handle_control_request`
- `daemon_delivery_kind`
- `egress_write_class`
- `tick` (`pub(super)` to `pub(crate)`)
- `DaemonEgressDiagnostics`
- `DaemonControlState.logical_clock`, `drain_cursors`, `egress_diagnostics`
- Moved Unix listener/connection/mux items that connection and `serve_daemon` call

## Runtime-teardown lenses

- Isolation: unchanged. One Unix connection still owns its mux, bound handles, route map, and pending close events. Shared slot and accumulator hold no cross-connection state.
- Bounds: `close` and `Drop` still return without waiting on socket I/O. Slot clear still uses `try_lock` with poisoned and would-block arms. Slice bounds still use `PUMP_MAX_*`.
- Late-message matrix: no row added, removed, or re-tagged.
- Production-path proof: Core still calls `try_write` on the exact adapter. Unix flush still reads `snapshot_writes` and `complete_active`. EOF still runs the connection cleanup guard. Existing lifecycle lanes stayed green.
- Ownership identity: route keys remain `(session_id, subscription_id, generation)`. Traversal still takes the live map key.
- Sibling fail-closed policy: Unix still does not sacrifice siblings. WebRTC bounded sibling sacrifice was not edited.

## Deviations from plan

1. One product commit landed the shared extraction, Unix move, WebRTC rebuild, scanner invariant, and guard retargets together. The plan preferred move-only commits then mechanical commits. The working tree mixed those slices; splitting after the fact would have been a synthetic history. Behavior is still move-only plus the required scanner/guard work.
2. Three new proof names exist because the plan required new source guards: `unix_listener_connection_and_mux_left_daemon_transport`, `shared_transport_contains_no_admission_route_grant_or_product_policy`, `shared_transport_declares_no_cross_transport_mux_or_route_record`. Every base leaf name remains with the same count.
3. Scanner needle repair uses `char::from_u32(0x7b)` plus concatenation rather than a comment-balanced `"{"` line. That keeps braces out of the test source.
4. `CloseSliceAccumulator` is the transport-neutral counter helper named in assumption 1. The loop, suppression check, classification, and wake stay in `closed_events.rs`.

No wire, DTO, limit, Core pin, or protocol change.

## Proof-name preservation (check 10a)

Base `a45cf7b` leaf-name multiset: 1294 unique names, 1297 rows including duplicates.
HEAD leaf-name multiset: 1297 unique names, 1300 rows.
Removed or count-reduced names: none.
Added names: the three new guards above.

Module-path rename map (leaf names unchanged):

- `unix_terminal_adapter::tests::*` → `transport::unix::adapter::tests::*`
- `daemon_transport::mux_write_resume_tests::*` → `transport::unix::mux_write::mux_write_resume_tests::*`

## Tests and downstream proof

All commands used `RUSTUP_TOOLCHAIN=1.97.0`. `rustc --version` was `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` was unset.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 after full rerun |
| Scanner skip-mode red arm on unrepaired `closed_events.rs` needles | `src/subscription/closed_events.rs production scanner must leave cfg(test) skip mode closed` |
| Same guard after needle repair | pass |
| Per-file GHOSTSNP list-entry ablation, 11 new `src/transport/**` files | each FAILED and named that exact file |
| Per-file GHOSTSNP tail ablation after the final `#[cfg(test)]` block, same 11 files | each FAILED and named that exact file |
| Scanner-liveness GHOSTSNP on `src/daemon_transport.rs` | `src/daemon_transport.rs production source must not contain GHOSTSNP` |
| Shared `ClosedEventRoute` red arm in `adapter_slot.rs` | `src/transport/shared/adapter_slot.rs production source must not contain ClosedEventRoute` |
| Both production Core conformance harnesses | pass in their homes |
| `mux_write_resume_tests` | all pass in `src/transport/unix/mux_write.rs` |
| `git diff --stat -- Cargo.toml Cargo.lock src/client_api_dto src/daemon/error.rs` | empty |
| Prebuild `botster-session-worker` and `botster-hub` | exit 0 |
| `./test.sh --locked` | exit 0. Lib 498 passed. Lifecycle 319 passed, 2 ignored. hub-client 81 passed. ui-contract 90 passed. No failures. |
| Generic-client cost | zero. No public export or UI-contract change. |

## Unverified behavior or residual risk

- The official suite ran once on a quiet host. First-run smoke attach flake from prior Hub tickets did not appear.
- Browser grant-secret derivation in production clients was not re-proven beyond existing Hub live WebRTC tests.
- `serve_daemon` still composes listener and connection from the control module. The next decomposition ticket still owns deleting `daemon_transport.rs`.

## Missing vault guidance discovered

Same four gaps the plan named. Not captured in this Implement visit; they belong in a later vault capture:

1. Region-bounded source guards need a positive anchor so an empty region fails.
2. Positive `contains` guards break loudly on a move; negative `!contains` guards go silently blind.
3. Unix and WebRTC adapters still differ in wake permit storage, deferred-flush filtering, close-from-host on `close_all`, and the test-only pressure setter.
4. Hub scanner skip-mode leaks are caused by brace counting inside string literals; concatenation is the local workaround.
