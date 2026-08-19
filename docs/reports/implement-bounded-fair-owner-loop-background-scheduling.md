# Implement report: Hub bounded fair owner-loop background scheduling

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786912569_840742` |
| Run | `run_1787102180_185677` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id`; worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | ticket branch `project-pipelines/ticket_1786912569_840742` |
| Base | Hub `origin/main` `8b4aeaf` |
| Locked Core | `Cargo.lock` pins `botster-core` at `8fce2041b9fe742cb2a6df9e74cb262606672742` |
| Delivery | direct-merge; no pull request (`merge_policy: direct`) |
| Class | not runtime-teardown |
| Plan | `docs/plans/implement-bounded-fair-owner-loop-background-scheduling.md` revision 3, with Implement notes for wake accounting and close-event retry |

Independent routing: `project_pipelines_current_context` and `botster context` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan uses the same routing. Work stayed in this ticket worktree.

Plan Review `review_1787112708_321608` approved revision 3 and named Core merge `8fce204` as the pin. Dependency `dependency_1787104278_385109` / Core `ticket_1787104273_140454` is closed.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[Hub background fairness must stay policy-neutral]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline artifacts should use path neutral worktree references]]

**Not loaded:** [[project-pipelines-playbook]] — this ticket does not change Project Pipelines package or plugin paths. [[botster runtime teardown lenses]] — teardown class does not apply.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Keep `MaintenanceScheduler` and `MaintenanceSliceKind` unchanged.
- One owner turn runs at most one background class slice.
- Do not add lifecycle-class client priority.
- Do not change terminal byte ownership or hub-client DTOs.
- Consume the two exact Core queries through the pinned Core artifact.
- Bind proof is one default-concurrency `./test.sh --locked` without retry.

## Files changed

Feature behavior:

- `src/daemon_maintenance.rs` — add policy-neutral `BackgroundClassScheduler`, `PumpScheduler`, and turn-decision helpers. Add deterministic unit proofs for class round-robin, coalesced marks, one-slice turns, no-cancellation, and composed `SubscriberDelivery` / `HostBridge` turn indices.
- `src/daemon_transport.rs` — `serve_daemon` serves at most one fairly selected background slice per turn. Pump is a three-phase rotation (`Observe`, `CloseEvents`, `InventoryReconcile`). Delete `observe_lifecycle_turn`. ReadScreen uses exact-session `observe_session_lifecycle`. ReadModeFlags and ShutdownSession do no broad observe. Close classification uses `session_registry_state`. Interval due marks Pump and wakes Maintenance. Successful control remakes Pump pending. After-control close-event queue stays, but uses the exact query.
- `src/daemon_attach_stream.rs` — add cursor-resumable `reconcile_inventory_slice` that validates routes with an exact membership lookup.
- `src/runtime.rs` — thin wrappers for `session_registry_state` and `terminal_subscription_generation`.
- `src/unix_terminal_adapter.rs` and `src/webrtc_terminal_adapter.rs` — bounded close-event visits with retry when the registry query is absent or errors.

Pin / fixture identity:

- `Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`, `crates/botster-hub-test-support/build.rs`, `crates/botster-hub-test-support/src/conformance_data.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/session_projection_owner_loop.rs`, and locked-core provenance strings — Core pin `8fce2041b9fe742cb2a6df9e74cb262606672742`.

Proof:

- `tests/hub_daemon_lifecycle/sessions.rs` — Status flood now also proves PTY output progress and complete producer output.

Handoff:

- `docs/plans/implement-bounded-fair-owner-loop-background-scheduling.md` — Implement notes for wake accounting and close-event retry.
- `docs/reports/implement-bounded-fair-owner-loop-background-scheduling.md` — this report.

## Ownership boundaries preserved

Hub owns owner-loop policy, budgets, and scheduling. Core remains the authority for lifecycle facts. The two exact queries are consumed through the pinned Core crate. Terminal bytes stay on the Core SessionIo / ClientWorker data plane. No `botster-hub-client` DTO fields changed. No Web, TUI, Workspaces, or Project Pipelines package/plugin paths were edited.

## Cross-repo routing

Registered Core dependency `ticket_1787104273_140454` / `dependency_1787104278_385109` is closed. This ticket consumes merged Core `8fce204` (`CoreDaemon::terminal_subscription_generation` and `CoreDaemon::session_registry_state`). No other separately routed work.

## Deviations from plan

1. `reconciliation_wakes` still increments only on a Maintenance slice. Counting Pump slices broke the idle `<= 4` oracle. Pump progress is proven by PTY output during the Status flood and by the ready-Spawn observation.
2. After-control close-event queue remains. It no longer calls `list_sessions` or `list_terminal_subscriptions`. It keeps mux flush prompt after control, including Status.
3. CloseEvents retries `Absent` and query error instead of marking the route reported. A live session that is not yet in the registry still emits `TerminalSubscriptionClosed` when Found(Running). Found(non-running) still suppresses.
4. A successful control request remakes Pump pending. Status waiters can then drive Observe / CloseEvents without waiting only on the 500 ms interval.
5. When the reconciliation interval is due, the loop also `try_wake`s Maintenance so package-entity fanout still gets a periodic slice. The turn still runs only one selected class.

## Tests and downstream proof

Production entry point: `serve_daemon` in `src/daemon_transport.rs` now classifies one control message, then at most one `BackgroundClass` slice. ReadScreen is the only remaining read path that observes lifecycle, and it uses the exact session.

Commands:

- `cargo fmt --all -- --check` — pass
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — pass
- `cargo test --doc --workspace --locked` — pass
- `./test.sh --locked` — one clean default-concurrency run, 253/0/1 on `hub_daemon_lifecycle_test`, no retry

Ready Spawn observation (not a pass/fail assert): `8.206125ms` from `ready_spawn_completes_when_live_sessions_exceed_one_observe_slice`. Below `MAX_READY_OPERATION_WAIT_MS` (50 ms).

## Unverified behavior or residual risk

- CloseEvents retry on Absent can re-visit a closed route until Found or until InventoryReconcile cancels it.
- Counting Pump in `reconciliation_wakes` is still the plan text. Changing that counter later needs a new idle oracle.
- Vault notes still say the replacement ticket owns the repair. Update them after merge.

## Missing vault guidance

None that blocked the work. After merge, update [[Hub background fairness must stay policy-neutral]] to shipped round-robin, and record that `observe_lifecycle_turn` is gone.
