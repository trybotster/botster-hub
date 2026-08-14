# Implement report: Emit core_adapter_closed while Unix host mux stays readable

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1786716545_417854` |
| Run | `run_1786717046_410510` |
| Step | `botster_stack_implement` (`run_step_1786721731_448860`) |
| Approved plan | `docs/plans/emit-core-adapter-closed-while-unix-host-mux-stays-readable.md` revision 3 |
| Merge policy | direct into `main`; do not create a PR |
| Base | worktree `HEAD` `aafd6c2cde430804f1bb54094c568fc88c15944b` |
| Locked Core | `Cargo.lock` pins `botster-core` `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `teardown_class_applies` | yes |
| Session-type eligibility consumer | false |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` maps `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. The approved plan used the same `target_id`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — public DTO overlay inside this repository; no DTO change
- [[botster runtime teardown lenses]] — required; class applies

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[Unix mux host events are unsolicited control frames]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[graceful-termination-requires-explicit-cleanup-hooks]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- Other repository charters (Core, Web, TUI, Workspaces, Ghostty)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 3; keep Hub charter ownership
- Do not edit Core, TUI, WebRTC, protocol version, or hub-test-support
- Runtime-teardown lenses are implemented, not deferred
- Unix adapter stays content-blind
- Use `./test.sh` for Rust tests; record exact-name executed vs filtered counts

## Files changed

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | One `MuxWriteState` for Response, Event, and Terminal; delivery_ack and close_after on queued Responses; host-first flush; abandon zero-progress terminal starts; ack only after Written; every Response waits in `flush_pending_responses` up to `DAEMON_CLIENT_WRITE_TIMEOUT`; `SubscribeEntities` is rejected with `unix_mux_owns_connection` while write state, unsent mux frames, or any bound route exists; reject does not `close_all`; queue close events before inventory reconcile |
| `src/unix_terminal_adapter.rs` | Flush-defer flag skipped by `snapshot_writes`; `close_from_host` does not claim host reason after Core already closed; `has_bound_routes` for the entity-subscription guard |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | IsolatedHub keep-reading proof: owned stall Drain then Status before close, exact `core_adapter_closed`, live sibling envelope, content-blind sibling Drain |
| `docs/client-protocol.md` | Host-readable / sibling-live / not-host-oracle wording |
| `docs/plans/emit-core-adapter-closed-while-unix-host-mux-stays-readable.md` | Approved plan revision 3 |
| `docs/reports/emit-core-adapter-closed-while-unix-host-mux-stays-readable-implement.md` | this report |

No `Cargo.lock`, protocol version, or hub-test-support version change.

## Ownership boundaries preserved

Hub owns Unix mux flush, route records, adapter handles, and host Event emit. Core still owns the 512-tick write-budget and adapter `close()`. Hub-client DTOs were consumed, not changed. TUI was not edited.

## Cross-repo dependencies or separately routed work

No new cross-repo dependency. Core `f4f6bf5` already emits the 512-tick close. Parent TUI ticket `ticket_1786661009_551067` remains the consumer. Sibling Hub-client pin ticket `ticket_1786716545_950076` is not this run.

## Deviations from plan

None. Two production-path details were required to keep the plan's close-reason rule:

1. `pump_bound_unix_routes` now queues `TerminalSubscriptionClosed` before `reconcile_inventory`. Reconcile first called `close_from_host` and made a keep-reading observer see `host_adapter_closed`.
2. `close_from_host` does not set `host_closed` when the adapter is already closed. A later Hub sweep must not rewrite Core close.

These preserve `host_closed` vs not. They do not add a third reason.

Review `review_1786720426_744133` required one more bound: every Response, not only `close_after`, stays in the write loop. The plan acceptance check was updated to match. That is not a silent deviation.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One mux route and its one-slot adapter die. Same-connection sibling stays bound and writable. `close_all()` remains connection death only. |
| Bounds | Mux writes stay 50ms slices. Every Response, including `DaemonShutdown` / `StartHubUpdate`, waits at most 2s. The connection does not read another request while a Response is pending. `SubscribeEntities` fails closed if write state, unsent mux frames, or any bound route exists. That reject keeps the mux and does not `close_all`. Terminal Pending does not `close_all`. Adapter `close()` stays non-blocking. |
| Late-message matrix | Existing host-close, Detach, connection-death, process-exit, stale-generation, and failed-RemoveSession tests still pass. A scoped stall Drain stays owned immediately before the pre-close Status. Status is proven after Core close. Sibling Drain stays owned. |
| Production-path proof | IsolatedHub Unix path through `CARGO_BIN_EXE_botster-hub` and `botster-hub-client::read_unix_mux_frame_from_reader`. Not a fixture. |
| Ownership identity | Event is `(session_id, subscription_id, generation)`. Stale generation N does not sweep N+1. |
| Sibling / fail-closed | Success path: sibling envelopes continue. Connection-death test still covers fail-closed `close_all` with no Event. |

## Tests and downstream proof run

Production entry points that use the new behavior:

- `handle_connection_async` enqueues Responses into `MuxWriteState` (HelloAck still uses the one-shot write)
- `flush_unix_mux_writes` host-first flush, abandon, and route defer
- `queue_unix_subscription_closed_events` after Core `close()` with `host_closed == false`

`./test.sh --offline --test hub_daemon_lifecycle_test unix_adapter` was not used. That filter runs 8 and skips 178, including this ticket's proof.

| Command | Result |
| --- | --- |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact core_write_budget_hard_stop_emits_core_adapter_closed` | 1 passed, 185 filtered, 7.32s after Drain oracle; 7.22s after restore |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact host_adapter_close_emits_terminal_subscription_closed_for_one_route` | 1 passed, 185 filtered |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact connection_death_and_detach_do_not_emit_terminal_subscription_closed` | 1 passed, 185 filtered |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` | 1 passed, 185 filtered |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact stale_generation_close_does_not_sweep_replacement_owner` | 1 passed, 185 filtered |
| `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact failed_remove_session_does_not_suppress_later_core_close` | 1 passed, 185 filtered |
| `./test.sh --offline -p botster-hub --lib mux_write_resume_tests` | 9 passed, 244 filtered (includes bound-route entity-subscription fail-closed) |
| `./test.sh --offline -p botster-hub --lib daemon_shutdown_waits_for_response_delivery_before_stopping` | 1 passed, 250 filtered |
| `./test.sh --offline -p botster-hub --lib daemon_shutdown_releases_when_delivery_owner_drops` | 1 passed, 250 filtered |
| `./test.sh --offline -p botster-hub --lib host_close_after_core_close_does_not_claim_host_reason` | 1 passed, 250 filtered |
| `./test.sh --offline -p botster-hub --lib deferred_route_is_omitted_from_snapshot_writes` | 1 passed, 250 filtered |
| `./test.sh --offline -p botster-hub-client --lib` | wrapper is `--workspace`; botster-hub 251, hub-client 70, installation 14, installer 14, test-support 44; all passed |
| `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` | passed |

IsolatedHub oracles observed on the live path:

- Scoped stall Drain owned after pressure starts and before `core_adapter_closed`
- Status Response on the same connection after that Drain and before the close Event
- Exact reason `core_adapter_closed`
- Status readable after the close
- Sibling `echo:cwb-sibling-live` envelope after the close
- Content-blind sibling Drain stayed owned (not `OperatorError`, no terminal bodies)
- Locked Core SHA `f4f6bf5babe92dfb9241a760c414187f711c2c42`
- Hub binary `CARGO_BIN_EXE_botster-hub` from this worktree; base SHA `aafd6c2`; implement commit `016536aa2e1d43823295fb9e8dc9106f28de7b2a`

Downstream: no public hub-client API change. TUI scratch `cargo check` was not required.

## Review findings addressed

| Finding | Fix |
| --- | --- |
| `finding_1786720427_976374` Blocked responses can grow the connection queue | `flush_pending_responses` waits for every Response with a 2s bound. The connection does not read another request while a Response is pending. |
| `finding_1786721717_230428` SubscribeEntities can still transition while the Unix mux owns unread output | One predicate: `mux_write.has_pending()` or `mux.has_unsent_mux_writes()` or `mux.has_bound_routes()`. Reject with `OperatorError` `unix_mux_owns_connection` and keep the mux. Do not `close_all`. |
| `finding_1786720427_613559` Pre-close Status does not prove the adapter is open | IsolatedHub now issues a scoped stall Drain and requires it to stay owned immediately before Status. |
| `finding_1786720427_607821` Required regression ablations were not executed | Narrow production ablations were run. Results are in the table below. |

## Ablations run (narrow enforcement, then restore)

Each row disabled one production decision, ran the named test, recorded the first red assertion, then restored the branch.

| Claim | Enforcement ablated | Command | Red assertion |
| --- | --- | --- | --- |
| Ordered mux lines | Skip resume of a nonzero-offset pending terminal before a queued Response | `./test.sh --offline -p botster-hub --lib partial_terminal_then_response_parses_two_complete_mux_lines` | `src/daemon_transport.rs:1007` `json line: Error("expected ':'", line: 1, column: 9)` |
| Ack after Written | Send `delivery_ack` inside `enqueue_response` | `./test.sh --offline -p botster-hub --lib partial_terminal_then_shutdown_response_acks_after_written` | `src/daemon_transport.rs:1152` `assertion failed: ack_rx.try_recv().is_err()` |
| Response wait bound | Return from `flush_pending_responses` after one flush | `./test.sh --offline -p botster-hub --lib stalled_response_stays_bounded_and_blocks_entity_subscription` | `src/daemon_transport.rs:1250` `a stalled Response must not return until Written or timeout` |
| Host-first flush | Write new terminal slots before Events | `./test.sh --offline -p botster-hub --lib host_event_flushes_before_new_terminal_slots` | `src/daemon_transport.rs:1117` `host Event must precede new terminal slots` with first frame `Terminal` for `stall` |
| Exact `core_adapter_closed` | Restore `close_from_host` on an already-closed adapter and reconcile before queue | `./test.sh --offline -p botster-hub --lib host_close_after_core_close_does_not_claim_host_reason` | `src/unix_terminal_adapter.rs:583` `a later host sweep must not rewrite Core close as host_adapter_closed` |
| Exact IsolatedHub reason | Same close-reason ablation | `./test.sh --offline --test hub_daemon_lifecycle_test -- --exact core_write_budget_hard_stop_emits_core_adapter_closed` | `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1231` `left: ["host_adapter_closed"]` |
| Route defer | Do not call `defer_flush` on abandon or backpressure | `./test.sh --offline -p botster-hub --lib zero_progress_terminal_start_is_abandoned_without_completing_slot` | `src/daemon_transport.rs:1065` `assertion failed: mux.snapshot_writes().is_empty()` |

IsolatedHub sibling envelope stayed green when defer was removed. Keep-reading plus host-first flush still delivered the sibling. Defer remains the unit-test isolation gate. After each ablation, the enforcement was restored before the next row.

## Unverified behavior or residual risk

- Full `hub_daemon_lifecycle_test` without a name filter was not run.
- IsolatedHub sibling delivery is not independently reddened by removing defer. The defer claim is proven by the zero-progress unit test.
- Socket-buffer fill vs 50ms abandon was not separately timed.
- Downstream TUI mux-read bugs remain TUI-owned (`finding_1786715974_898936`).

## Missing vault guidance discovered

[[Unix mux host events are unsolicited control frames]] records one pending frame and content-blind Event emit. It did not record:

- Host Response/Event must flush before new terminal slots, or a flood occupies the mux and becomes `host_adapter_closed`.
- `write_async_frame` beside a nonzero-offset pending terminal line can split a JSON mux frame.
- `ListSessions` lifecycle `running` is not sibling terminal delivery.
- `reconcile_inventory` `close_from_host` after Core inventory removal can rewrite `core_adapter_closed` unless the Event is queued first and host close does not claim an already-closed adapter.

Captured to vault inbox after Implement.
