# Implement report: Hub bounded fair owner-loop background scheduling

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786912569_840742` |
| Run | `run_1787102180_185677` |
| Step | `botster_stack_implement` (return from Review) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id`; worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | ticket branch `project-pipelines/ticket_1786912569_840742` |
| Base | Hub `origin/main` `8b4aeaf` |
| Locked Core | `Cargo.lock` pins `botster-core` at `8fce2041b9fe742cb2a6df9e74cb262606672742` |
| Delivery | direct-merge; no pull request (`merge_policy: direct`) |
| Class | not runtime-teardown |
| Plan | `docs/plans/implement-bounded-fair-owner-loop-background-scheduling.md` revision 6 |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Work stayed in this ticket worktree.

This report answers Review `review_1787131785_727118` (two open findings) after prior returns `review_1787126462_489606`, `review_1787124251_499447`, and `review_1787118229_406859`. Plan Review `review_1787112708_321608` approved revision 3. Revision 6 restores the unchanged nine-kind rotation.

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
- [[implementation deviations must resync committed plan acceptance checks]]
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

## Review findings addressed

- `finding_1787131785_826374`: `prefer_observe` is removed. An incomplete Observe pass stores `observe_resume` and leaves the nine-kind pointer unchanged. `observe_resume` is not `needs_work`, so it does not rearm the whole rotation as idle wakes. The composed test with a continuously incomplete Observe pass still executes HostBridge at turn 8 and SubscriberDelivery at turn 10.
- `finding_1787131785_814522`: plan revision 6 restates that the `MaintenanceSliceKind` round-robin is unchanged. The 18-turn composed fairness acceptance check stays in force.

## Files changed

Feature behavior:

- `src/daemon_maintenance.rs` — nine-kind rotation unchanged after incomplete Observe; `observe_resume` and `projection_dirty` are not self-wakes; sealed `ObservePassUnavailable` does not remint; HostBridge drains budget-fitting fanout jobs.
- `src/daemon_entity_subscriptions.rs` — session delivery pending is `needs_delivery` or resync, not `projection_dirty`. SubscriberDelivery does not increment `reconciliation_wakes` (the Maintenance class already counts the slice).
- `src/daemon_transport.rs` — one control then at most one class slice. Pump phases `Observe` → `CloseEvents` → `InventoryReconcile`. No after-control mux scan. No Status/ReadModeFlags Pump remake. Close work marks Pump only.
- `src/daemon_attach_stream.rs` — `reconcile_inventory_slice` uses `BTreeMap::range` and counts unbound rows toward the visit budget. One stale cancel removes only that client's keyed route.
- `src/runtime.rs` — thin wrappers for `session_registry_state` and `terminal_subscription_generation`.
- `src/unix_terminal_adapter.rs` and `src/webrtc_terminal_adapter.rs` — mux routes are `BTreeMap`s. Close-event slices resume with an exclusive route cursor and a visited-entry budget. Suppression is a keyed set lookup. Adapter close sets a coalesced close-work flag.

Pin / fixture identity (unchanged from the first Implement pass):

- Core pin `8fce2041b9fe742cb2a6df9e74cb262606672742`.

Proof:

- `tests/hub_daemon_lifecycle/sessions.rs` — Status flood PTY progress; eight-session idle sample waits 1.2 s and prints wake/change/delivery/drain deltas.
- `src/daemon_maintenance.rs` tests — sealed unavailable observe; incomplete unavailable recovery; incomplete Observe keeps the nine-kind rotation; composed incomplete Observe still serves HostBridge and SubscriberDelivery; `observe_resume` and `projection_dirty` alone are not `needs_work`.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` — Unix Core-close wait matches the WebRTC 20s bound so CloseEvents can run after adapter close without a Status-rearm starvation path.

Handoff:

- `docs/plans/implement-bounded-fair-owner-loop-background-scheduling.md` — revision 6.
- `docs/reports/implement-bounded-fair-owner-loop-background-scheduling.md` — this report.

## Ownership boundaries preserved

Hub owns owner-loop policy, budgets, and scheduling. Core remains the authority for lifecycle facts. The two exact queries are consumed through the pinned Core crate. Terminal bytes stay on the Core SessionIo / ClientWorker data plane. No `botster-hub-client` DTO fields changed. No Web, TUI, Workspaces, or Project Pipelines package/plugin paths were edited.

## Cross-repo routing

Registered Core dependency `ticket_1787104273_140454` / `dependency_1787104278_385109` is closed. This ticket consumes merged Core `8fce204`. No other separately routed work.

## Deviations from plan

1. `reconciliation_wakes` counts Maintenance slices only. Pump progress is the flood PTY oracle and the ready-Spawn observation. Plan revision 4 records this.
2. CloseEvents retries `Absent` and query error instead of marking the route reported. Found(Running) still emits. Found(non-running) still suppresses.
3. Adapter close sets one coalesced close-work flag and marks Pump. It does not rewrite the Pump phase pointer.
4. The Unix Core-close waiter uses a 20s bound, matching WebRTC. The previous 8s bound depended on Status-rearm and an unbounded after-control scan starving drain.
5. Idle wake accounting no longer treats `observe_resume` as `needs_work`. The nine-kind pointer still advances. HostBridge and SubscriberDelivery keep their 18-turn bound.

## Tests and downstream proof

Production entry point: `serve_daemon` in `src/daemon_transport.rs` serves one control message, then at most one `BackgroundClass` slice. CloseEvents is not invoked from the control path. ReadScreen is the only remaining read path that observes lifecycle, and it uses the exact session.

Commands (one clean run, no retry):

- `cargo fmt --all -- --check` — pass
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` — pass
- `./test.sh --locked` — pass; `hub_daemon_lifecycle_test` 253 passed, 0 failed, 1 ignored. Command matches Review: no `--offline`, no retry, default concurrency.

Ready Spawn observation from the first Implement pass: `8.206125ms` (below 50 ms). The return gate re-ran the ready-Spawn tests and they stayed green.

## Unverified behavior or residual risk

- CloseEvents retry on Absent can re-visit a closed route until Found or until InventoryReconcile cancels it.
- Vault notes still say the replacement ticket owns the repair. Update them after merge.

## Missing vault guidance

None that blocked the work. After merge, update [[Hub background fairness must stay policy-neutral]] to shipped round-robin, and record that `observe_lifecycle_turn` is gone.
