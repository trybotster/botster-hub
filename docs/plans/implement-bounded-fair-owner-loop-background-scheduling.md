# Plan: implement bounded fair owner-loop background scheduling

Ticket: `ticket_1786912569_840742` — Hub: implement bounded fair owner-loop background scheduling.
Run: `run_1787102180_185677`. Base: `origin/main` (`8b4aeaf`).

## Target

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- The ticket replaces the scheduler portion of superseded `ticket_1786875812_242946`. Work starts from current `main`. Do not cherry-pick the superseded branch.

## Context loaded

- Repository playbook: [[botster-hub-playbook]].
- Role playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Targeted atomic notes:
  - [[Hub background fairness must stay policy-neutral]]
  - [[Owner loop must not stack maintenance and pump ahead of queued control]]
  - [[Hub owner loop calls bounded Core lifecycle page APIs]]
  - [[Hub owner loop wakes only for mutations and pending resync]]
  - [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
  - [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
  - [[observed-exit waits must issue a production exact-session observe turn]]
  - [[host ShutdownSession classification must call the exact-session Core query]]
  - [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- Repository code surveyed: `src/daemon_transport.rs` (owner loop, `pump_bound_unix_routes`, `observe_lifecycle_turn`, operation handlers), `src/daemon_maintenance.rs` (`MaintenanceScheduler`, `MaintenanceSliceKind`, budgets, `needs_work`), `src/runtime.rs` (`observe_session_lifecycle`, `read_screen`), `src/client_api.rs` (ReadScreen path), `tests/hub_daemon_lifecycle/sessions.rs` (`focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible`).
- Core pin: `302c7f7` (Cargo.toml). The pin already exposes `observe_session_lifecycle`, `observe_lifecycle_slice`, `lifecycle_baseline_page`, and `take_journal_advanced_wake`.
- Runtime-teardown class: does not apply. This ticket changes owner-loop scheduling. It does not change WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, or terminal-state divergence surfaces. [[botster runtime teardown lenses]] is intentionally not loaded.

## Current defects (from code survey)

1. One owner turn stacks background work. `serve_daemon` serves one control message, then runs one maintenance slice, then runs `pump_bound_unix_routes` in the same turn when the reconciliation interval is due (`src/daemon_transport.rs:367-374`).
2. `pump_bound_unix_routes` stacks more work inside itself. On a journal wake it runs `JournalPull` and `ProjectionApply` inline (`src/daemon_transport.rs:5038-5050`).
3. The pump observe uses an inline budget of 32 sessions / 25 ms instead of `OBSERVE_SLICE_BUDGET` (8 sessions / 64 KiB / 8 ms) (`src/daemon_transport.rs:5016-5024`).
4. Operation paths perform broad lifecycle observation. `observe_lifecycle_turn` (32 sessions / 25 ms) runs inline on WebRTC Attach bind (`:3264`), Unix Attach bind (`:3310`), ShutdownSession (`:3431`), ReadScreen (`:3498`), ReadModeFlags (`:3515`), and `recover_after_core_shutdown_error` (`:4519`).
5. There is no explicit Pump work class. Pump work runs only when the 500 ms interval is due, so its progress is not tracked as coalesced pending work.

## Scope

Implement one explicit bounded scheduler for the two Hub owner-loop background classes: **Maintenance** (the existing `MaintenanceSliceKind` round-robin) and **Pump** (the bound-route pump: inventory reconcile, closed-event queueing, and the bounded observe slice).

### 1. Background-class scheduler (`src/daemon_maintenance.rs`)

- Add an owner background scheduler that tracks one coalesced pending flag per class. Marks coalesce; they do not queue.
- Maintenance pending remains `MaintenanceState::needs_work()` plus the existing `MaintenanceScheduler` wake. Keep `MaintenanceScheduler` and `MaintenanceSliceKind` unchanged so existing slice-harness tests keep working.
- Add a coalesced Pump pending flag with its mark sources (listed in section 3).
- Fair rule (documented in module docs): **round-robin between classes**. The scheduler stores the last-served class. When both classes are pending, it selects the class that did not run last. When one class is pending, it selects that class. Selection consumes the pending flag for the selected class only.
- The scheduler must not read client identity, session type, or lifecycle class. Selection input is only {pending flags, last-served class}.
- A selected slice runs to its bounded completion. Queued control messages do not cancel a selected slice.

### 2. Owner loop turn shape (`src/daemon_transport.rs`, `serve_daemon`)

- Keep `classify_owner_poll` control precedence: a queued control message is served before a due background slice.
- One owner turn runs **at most one** background slice:
  - Turn with queued control: serve one control message, then run one fairly selected background slice when any class is pending.
  - Turn with no queued control and a pending class: run one fairly selected background slice.
  - Otherwise block on the control channel with the reconciliation deadline (existing `receive_owner_event`).
- When the reconciliation interval is due, the loop marks Pump pending and advances `next_reconciliation`. The loop does not run the pump inline after a maintenance slice. This removes the stacked path at `src/daemon_transport.rs:367-374`.
- `reconciliation_wakes` increments once per executed background slice of either class, so the existing counter-progress oracles keep observing background progress.

### 3. Pump slice content

- Rename/adapt `pump_bound_unix_routes` into the Pump-class slice. Keep: `list_terminal_subscriptions` inventory reconcile, `queue_unix_subscription_closed_events`, `queue_webrtc_subscription_closed_events`, `lifecycle_session_drains` counter.
- The pump observe call uses `OBSERVE_SLICE_BUDGET` (unchanged constant: 8 sessions, 64 KiB, `max_elapsed` 8 ms) with the existing `observe_resume` cursor. An incomplete slice keeps Pump pending.
- On `take_journal_advanced_wake`, the pump calls `note_authoritative_mutation()` only. It does not run `JournalPull` or `ProjectionApply` inline. The next fairly selected Maintenance slice performs the pull.
- Pump pending mark sources: reconciliation interval due; successful Attach bind (Unix and WebRTC); successful Spawn/SpawnSessionType acknowledgement; incomplete previous pump slice; subscription-close reconciliation need.

### 4. Operation paths lose broad lifecycle observation

- Delete `observe_lifecycle_turn`.
- Unix and WebRTC Attach bind: replace the broad observe with a Pump pending mark. The same control turn then runs one background slice, so first output pumping stays prompt without stacking.
- ReadScreen and ReadModeFlags: replace the broad observe with one exact-session `HubRuntime::observe_session_lifecycle(&session_id, now)` call before the read. This preserves the observed-exit wait contract from [[observed-exit waits must issue a production exact-session observe turn]] with a stimulus that targets the named session instead of a 25 ms collection scan. On a resulting journal wake, mark Maintenance pending; do not pull inline.
- ShutdownSession: remove the broad observe before `classify_shutdown_session`. Classification already calls the exact-session Core query per [[host ShutdownSession classification must call the exact-session Core query]]. Keep the typed `Found`/`Absent`/`Err` split unchanged.
- `recover_after_core_shutdown_error`: remove the broad observe. The exact-session classify inside the recover path supplies the observation.
- Spawn, SpawnSessionType, Drain, Input, and Resize already perform no lifecycle observation; verify this stays true.

### 5. Preserved constants and behavior

- `MAX_OWNER_TURN_MS = 25`, `MAX_READY_OPERATION_WAIT_MS = 50`, `OBSERVE_SLICE_BUDGET.max_elapsed = 8 ms`, `BASELINE_PAGE_BUDGET` — all unchanged.
- `seed_lifecycle_reconciliation` startup seeding and its `lifecycle_baseline_reads == 1` oracle — unchanged.
- The `MaintenanceSliceKind` round-robin order — unchanged. SubscriberDelivery and HostBridge keep receiving bounded progress through that rotation, now guaranteed a share of turns by class-level fairness.
- Terminal byte ownership — unchanged. The Hub still never decodes or branches on terminal frames; the pump drains through the existing Core pending-drain path.

## Non-scope

- No lifecycle-class client priority of any kind.
- No changes to Core (`botster-core`) or to the Core pin.
- No `botster-hub-client` DTO changes.
- No changes to session snapshot paging (owned by merged `ticket_1786912570_127968`).
- No PTY process-fixture rework (owned by `ticket_1786912572_610381`).
- No changes to the package event router, `events.emit` ingress, or host-control write fairness.
- If a distinct failure appears during gates, file a separate blocker ticket instead of expanding this ticket.

## Ownership boundaries and cross-repo dependencies

- All changes stay in `botster-hub`. The Hub owns owner-loop policy, budgets, and scheduling (host profile charter).
- Core stays authoritative for lifecycle facts. The plan consumes only existing pinned Core APIs (`observe_lifecycle_slice`, `observe_session_lifecycle`, `take_journal_advanced_wake`, journal pages). No new Core surface is required, so no cross-repo dependency is registered.
- Terminal bytes stay on the Core SessionIo/ClientWorker data plane. The scheduler selects when the Hub asks Core to pump; it never touches frame content.

## Assumptions and unknowns

- Assumption: `observe_session_lifecycle` at Core pin `302c7f7` reconciles a parked `ProcessExited` for the named session (charter note; `CoreDaemon::observe_session_lifecycle` exists at `daemon.rs:829` in the pinned source).
- Assumption: replacing the ReadScreen broad slice with the exact-session observe strengthens, not weakens, the observed-exit wait stimulus. The vault note records that the broad 25 ms slice alone failed 4 of 30 targeted runs because it can end before reaching the target session.
- Unknown: whether any existing test depends on incidental cross-session lifecycle advancement from the removed broad observes on Attach/ReadScreen paths. Mitigation: run the full default-concurrency suite; repair any such test with an exact-session stimulus; if a production seam turns out to be missing, stop and file a blocker instead of widening this ticket.
- Unknown: exact pump-progress cadence under continuous both-classes-pending load (each class gets every other slice). The sustained-traffic integration proof covers this.

## Affected surfaces/files

- `src/daemon_maintenance.rs` — new background-class scheduler type plus unit tests; `MaintenanceScheduler` untouched.
- `src/daemon_transport.rs` — `serve_daemon` turn shape; `run_one_owner_maintenance_slice` becomes the class-dispatched slice runner; pump slice rework; removal of `observe_lifecycle_turn` and its six call sites; exact-session observe on ReadScreen/ReadModeFlags; decision-level tests.
- `tests/hub_daemon_lifecycle/sessions.rs` — sustained-control-traffic proof extended with PTY progress; ready-Spawn proof.
- Existing tests touching removed behavior may need stimulus repairs (see unknowns).

## Risks

1. **Observed-exit wait regressions.** History: 2/28 and 4/30 flake rates on this contract. The exact-session observe is the note-mandated stronger stimulus, but the suite must prove it under default concurrency.
2. **Projection latency shift.** Removing inline `JournalPull`/`ProjectionApply` from the pump defers projection by one background turn. `projection_caught_up` gating for first snapshots must stay green.
3. **Idle-bound counters.** `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible` asserts idle `lifecycle_change_reads <= 4` and no baseline rescans. Pump-as-class must not add idle work.
4. **Busy-mark loop.** Marking Pump pending on interval-due must advance the deadline at mark time, or the loop would spin on an always-due interval.
5. **Wall-clock oracles.** New tests must use work bounds and decision-level oracles, not elapsed time, per the two wall-clock gotcha notes.

## Acceptance checks/tests

Deterministic scheduler state tests (unit):
- Both classes pending: selection alternates Pump/Maintenance across consecutive turns (round-robin proof over a fixed sequence).
- One class pending: that class runs on consecutive turns without requiring the other.
- Marks coalesce: repeated marks produce one pending flag, one slice.
- One turn, one slice: the turn-decision function never returns two background slices for one turn, including when the reconciliation interval is due during a maintenance-pending turn.
- Control precedence: extend `queued_control_precedes_a_due_maintenance_slice` to cover both classes pending.
- No cancellation: a selected slice decision survives a control message that arrives after selection (decision-level proof; negative control with the rule inverted must go red).

Integration proof (`tests/hub_daemon_lifecycle`):
- Sustained control traffic: pipelined Status flood while a live PTY producer emits output. Assert `reconciliation_wakes` strictly increases **and** PTY output progresses (screen/drain content advances) during the flood. Progress deltas, not elapsed time, are the oracle.
- Ready Spawn: with a live-session backlog larger than one observe slice, a Spawn completes successfully; the decision-level oracle proves control precedence; the end-to-end elapsed time is recorded as observational evidence with the expectation below 50 ms (per [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]], elapsed time is evidence, not the deterministic gate).
- No terminal output loss: existing attach output-retention tests stay green; the flood-PTY test asserts the complete expected producer output is eventually received.
- Observed-exit waits and shutdown classification tests stay green under the exact-session stimulus.
- `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible` stays green (idle bounds plus wake progress).

Repository gates (all must pass, one clean run each):
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --doc --workspace`
- `./test.sh --locked` at default concurrency, one clean run without retry. A distinct failure becomes a separate blocker ticket.

## Vault gaps

- After merge: update [[Hub background fairness must stay policy-neutral]] from "replacement ticket owns the repair" to shipped, and record the chosen round-robin class rule.
- Capture that `observe_lifecycle_turn` and per-operation broad observation are removed, so future operation-path work does not reintroduce them.
- [[observed-exit waits must issue a production exact-session observe turn]] describes the exact-session ReadScreen call as required; before this ticket the repo shipped only the broad-slice form. Capture the shipped resolution.
