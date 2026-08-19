# Plan: implement bounded fair owner-loop background scheduling

Ticket: `ticket_1786912569_840742` — Hub: implement bounded fair owner-loop background scheduling.
Run: `run_1787102180_185677`. Base: `origin/main` (`8b4aeaf`).

Revision 2 answered Plan Review `review_1787103654_733071`:
- `finding_1787103654_765870`: ReadModeFlags loses lifecycle observation entirely (section 4).
- `finding_1787103654_982441`: the Pump class becomes a bounded phase rotation with continuation cursors and per-slice work caps (section 3).
- `finding_1787103654_131792`: acceptance adds a deterministic composed-fairness proof for SubscriberDelivery and HostBridge, and implementation evidence must record one actual ready-Spawn observation below 50 ms (Acceptance section).

Revision 3 answers Plan Review `review_1787104175_260710`:
- `finding_1787104176_184650`: the missing Core API is a registered dependency now, not a measurement-deferred trigger. Core ticket `ticket_1787104273_140454` (target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) adds an exact subscription-membership query and an exact non-mutating registry-state query; dependency `dependency_1787104278_385109` blocks this ticket on it. InventoryReconcile drops the full-list frozen snapshot and validates routes with the exact query (section 3).
- `finding_1787104176_752235`: CloseEvents uses the exact **non-mutating** registry-state query instead of `observe_session_lifecycle`, so it cannot advance lifecycle or raise a journal wake; a behavior matrix (running / exited / shutdown / query-error) plus a no-wake proof enters acceptance (sections 3 and Acceptance).
- `finding_1787104176_111090`: the existing vault checklist items are updated in place with revision context; no new checklist.

Revision 4 answers Implement Review `review_1787118229_406859`:
- `finding_1787118229_281425`: remove the after-control full-set close-event scan. CloseEvents runs only as a Pump phase.
- `finding_1787118229_230218`: admission, mux-route, and stream cursors resume with `BTreeMap::range` plus a visit budget for open, reported, unbound, and pre-cursor prefixes.
- `finding_1787118229_191801`: Status and ReadModeFlags no longer remake Pump. Mark sources are the documented Pump work sources only.
- `finding_1787118229_310222`: keep idle `reconciliation_wakes` `<= 4` by not self-rearming Pump from reads.
- `finding_1787118229_810551`: this plan now matches Maintenance-only wake accounting.

Implement notes (revision 4, answering Review `review_1787118229_406859`):
- `reconciliation_wakes` increments once per executed Maintenance slice. Pump progress is the flood PTY oracle and the ready-Spawn observation. Interval due marks Pump and wakes Maintenance. Status and ReadModeFlags do not remake Pump.
- Control turns do not scan muxes for close events. CloseEvents runs only as a selected Pump phase.
- CloseEvents retries Absent and query error instead of marking reported.
- Pump is marked only at documented sources: interval due, successful Spawn/SpawnSessionType/Attach, Detach/ShutdownSession/RemoveSession (including failed RemoveSession), and an incomplete Pump phase. Status, ReadModeFlags, ReadScreen, and other reads do not remake Pump.
- CloseEvents and InventoryReconcile resume with `BTreeMap::range` and a per-slice visit budget, so open, reported, unbound, and pre-cursor prefixes cannot make one slice O(total rows).
- Adapter close and close-related control only mark Pump. They do not rewrite the Pump phase pointer. CloseEvents stays in the three-phase rotation so Observe and InventoryReconcile still run under continuous close work.
- CloseEvents suppression uses keyed sets and per-candidate lookup. InventoryReconcile removes one client's route by reverse owner lookup, not a full ledger scan.
- A sealed baseline does not remint from `ObservePassUnavailable`. Interval still marks Pump and wakes Maintenance.
- Interval due also wakes Maintenance. One turn still runs one class.

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
- `reconciliation_wakes` increments once per executed Maintenance slice. Interval due marks Pump and wakes Maintenance so package-entity fanout and the idle/flood oracles share one counter. Pump does not increment it.

### 3. Pump class: a bounded phase rotation

The current `pump_bound_unix_routes` performs full-set work in one call: a full Core `list_terminal_subscriptions`, a full scan of every Unix and WebRTC admission, one full `runtime.list_sessions()` **per close candidate** inside `session_suppresses_terminal_subscription_closed` (`src/daemon_transport.rs:4641`), and a full `reconcile_inventory` of every bound stream against the full inventory. Reclassifying that call as one slice would not be bounded. Instead the Pump class becomes its own rotation of three bounded phases. One Pump selection runs exactly one phase. Each phase has continuation state and per-slice work caps, mirroring the `MaintenanceSliceKind` pattern.

Both non-observe phases consume the two exact queries delivered by registered Core dependency `ticket_1787104273_140454`: an exact terminal-subscription membership query for one `(SessionId, SubscriptionId)`, and an exact **non-mutating** session registry-state query for one `SessionId`. The owner loop stops calling `list_terminal_subscriptions` and stops calling `list_sessions` for close classification.

**PumpPhase::CloseEvents**
- Visits admissions in key order with a resume cursor over the admission maps (`unix_admissions`, then `webrtc_admissions`). The next admission is `BTreeMap::range` from the exclusive cursor, not `keys().find` from the map start.
- Per-slice caps: `max_admissions_visited`, `max_candidate_classifications`, and `max_route_entries_visited` (constants, same order of magnitude as the existing slice caps such as `CONSUMER_REFRESH_MAX = 8`). Mux scans count open and already-reported rows toward the entry budget and resume with a route cursor.
- The suppression predicate stops calling the full `runtime.list_sessions()` per candidate. It classifies each candidate with the exact **non-mutating** registry-state query. Semantics are preserved exactly against today's predicate (`src/daemon_transport.rs:4641`): a `Running` state does not suppress, so the running-session adapter close still emits `TerminalSubscriptionClosed`; any non-running state (stopping, exited, stale) suppresses; an absent session suppresses; a query error suppresses (suppress-safe, matching today's `Err -> true` fallback).
- Because the query is non-mutating, CloseEvents cannot advance session lifecycle and cannot raise a journal-advanced wake. Wake handling stays in one place: only `PumpPhase::Observe` and the exact-session observes on ReadScreen/ShutdownSession can raise the journal wake, and every site that sees `take_journal_advanced_wake` marks Maintenance pending (`note_authoritative_mutation`) without pulling inline.
- An incomplete visit keeps Pump pending and resumes after the cursor.

**PumpPhase::InventoryReconcile**
- Purpose unchanged: close Hub-bound routes whose Core subscription vanished (the backstop behind mux close events).
- Continuation slices walk the Hub stream map with `BTreeMap::range` from the exclusive cursor and visit at most `max_routes_validated` map entries per slice, including unbound rows. Bound rows among those entries are validated with the exact subscription-membership query: explicit absence, or a generation mismatch under the existing `reconcile_inventory` rule (`src/daemon_attach_stream.rs:339`), closes the adapter and cancels the stream. No full inventory list, no snapshot, no sort, no prefix rescan.
- This removes the previous revision's frozen-snapshot design and its O(n log n) `list_terminal_subscriptions` acquisition. The reviewer confirmed at pinned Core `302c7f7` that the full-list call clones and sorts every live row, so it cannot be bounded from the Hub side; the exact query from `ticket_1787104273_140454` is the bounded replacement.

**PumpPhase::Observe**
- The observe call uses `OBSERVE_SLICE_BUDGET` (unchanged constant: 8 sessions, 64 KiB, `max_elapsed` 8 ms) with the existing `observe_resume` cursor. An incomplete slice keeps Pump pending.
- On `take_journal_advanced_wake`, the phase calls `note_authoritative_mutation()` only. It does not run `JournalPull` or `ProjectionApply` inline. The next fairly selected Maintenance slice performs the pull.

Pump pending mark sources: reconciliation interval due; successful Attach bind (Unix and WebRTC); successful Spawn/SpawnSessionType acknowledgement; incomplete previous pump phase; Detach, ShutdownSession, and RemoveSession (close or inventory-reconcile need). Status, ReadModeFlags, ReadScreen, ListSessions, and other reads do not mark Pump.

### 4. Operation paths lose broad lifecycle observation

- Delete `observe_lifecycle_turn`.
- Unix and WebRTC Attach bind: replace the broad observe with a Pump pending mark. The same control turn then runs one background slice, so first output pumping stays prompt without stacking.
- ReadScreen: replace the broad observe with one exact-session `HubRuntime::observe_session_lifecycle(&session_id, now)` call before the read. This preserves the observed-exit wait contract from [[observed-exit waits must issue a production exact-session observe turn]] with a stimulus that targets the named session instead of a 25 ms collection scan. On a resulting journal wake, mark Maintenance pending; do not pull inline. ReadScreen is the only read operation that keeps an observation, because the observed-exit note names it as the wait stimulus.
- ReadModeFlags: remove lifecycle observation entirely. No note requires ReadModeFlags to observe lifecycle, and the ticket removes lifecycle work from operation paths. Add a check that the ReadModeFlags path performs no lifecycle observation: a behavior test that issues ReadModeFlags and asserts the observe counters (`lifecycle_session_drains`, `reconciliation_wakes`, journal reads) do not move, or an equivalent source-scan assertion beside the existing control-plane architecture checks.
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
- No Core code changes inside this ticket. The two exact queries are delivered by the registered Core dependency `ticket_1787104273_140454`; this ticket only bumps the Core pin to consume the merged revision.
- No `botster-hub-client` DTO changes.
- No changes to session snapshot paging (owned by merged `ticket_1786912570_127968`).
- No PTY process-fixture rework (owned by `ticket_1786912572_610381`).
- No changes to the package event router, `events.emit` ingress, or host-control write fairness.
- If a distinct failure appears during gates, file a separate blocker ticket instead of expanding this ticket.

## Ownership boundaries and cross-repo dependencies

- All changes stay in `botster-hub`. The Hub owns owner-loop policy, budgets, and scheduling (host profile charter).
- Core stays authoritative for lifecycle facts. The Hub scheduler consumes existing pinned Core APIs (`observe_lifecycle_slice`, `observe_session_lifecycle`, `take_journal_advanced_wake`, journal pages) plus the two exact queries from the registered Core dependency.
- **Registered cross-repo dependency:** Core ticket `ticket_1787104273_140454` (target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) adds the exact subscription-membership query and the exact non-mutating registry-state query. Dependency `dependency_1787104278_385109` blocks this Hub ticket on it. The Hub Implement step must bump the Core git pin in `Cargo.toml` (currently `302c7f7`) to a merged Core `main` revision that contains both queries, and must consume them through that pinned artifact. Hub implementation of sections 3's non-observe phases cannot start before that artifact exists.
- Terminal bytes stay on the Core SessionIo/ClientWorker data plane. The scheduler selects when the Hub asks Core to pump; it never touches frame content.

## Assumptions and unknowns

- Assumption: `observe_session_lifecycle` at Core pin `302c7f7` reconciles a parked `ProcessExited` for the named session (charter note; `CoreDaemon::observe_session_lifecycle` exists at `daemon.rs:829` in the pinned source).
- Assumption: replacing the ReadScreen broad slice with the exact-session observe strengthens, not weakens, the observed-exit wait stimulus. The vault note records that the broad 25 ms slice alone failed 4 of 30 targeted runs because it can end before reaching the target session.
- Unknown: whether any existing test depends on incidental cross-session lifecycle advancement from the removed broad observes on Attach/ReadScreen paths. Mitigation: run the full default-concurrency suite; repair any such test with an exact-session stimulus; if a production seam turns out to be missing, stop and file a blocker instead of widening this ticket.
- Unknown: exact pump-progress cadence under continuous both-classes-pending load (each class gets every other slice). The sustained-traffic integration proof covers this.
- Assumption: Core's internal exact membership primitives (named by Plan Review) let `ticket_1787104273_140454` deliver both queries without touching subscription lifecycle or journal semantics; the Core ticket's own gates prove this.

## Affected surfaces/files

- `src/daemon_maintenance.rs` — new background-class scheduler type plus unit tests; `MaintenanceScheduler` untouched.
- `src/daemon_transport.rs` — `serve_daemon` turn shape; `run_one_owner_maintenance_slice` becomes the class-dispatched slice runner; Pump phase rotation with continuation state; removal of `observe_lifecycle_turn` and its six call sites; exact-session observe on ReadScreen only; bounded close-candidate classification replacing the per-candidate `list_sessions` predicate; decision-level and work-bound tests.
- `src/daemon_attach_stream.rs` — `reconcile_inventory` gains a bounded, cursor-resumable form validating routes with the exact membership query.
- `src/runtime.rs` — thin `HubRuntime` wrappers for the two new Core queries.
- `Cargo.toml` — Core git pin bump to the merged dependency revision.
- `tests/hub_daemon_lifecycle/sessions.rs` — sustained-control-traffic proof extended with PTY progress; ready-Spawn proof; ReadModeFlags no-observe check.
- Existing tests touching removed behavior may need stimulus repairs (see unknowns).

## Risks

1. **Observed-exit wait regressions.** History: 2/28 and 4/30 flake rates on this contract. The exact-session observe is the note-mandated stronger stimulus, but the suite must prove it under default concurrency.
2. **Projection latency shift.** Removing inline `JournalPull`/`ProjectionApply` from the pump defers projection by one background turn. `projection_caught_up` gating for first snapshots must stay green.
3. **Idle-bound counters.** `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible` asserts idle `lifecycle_change_reads <= 4` and no baseline rescans. Pump-as-class must not add idle work.
4. **Busy-mark loop.** Marking Pump pending on interval-due must advance the deadline at mark time, or the loop would spin on an always-due interval.
5. **Wall-clock oracles.** New tests must use work bounds and decision-level oracles, not elapsed time, per the two wall-clock gotcha notes.
6. **Dependency timing.** The Core dependency ticket must merge before Hub Implement can build the non-observe Pump phases; the Hub pin bump also pulls any other Core `main` changes merged since `302c7f7`, so the Implement step re-runs the full Hub suite against the new pin.
7. **Close-candidate classification change.** The suppression predicate must preserve today's behavior exactly: running emits the close event, non-running/absent suppresses, and a query error stays suppress-safe (`Err -> true`). The acceptance matrix covers all four cases.
8. **Chunked pump latency.** Splitting the pump into three rotated phases lengthens the interval between observe slices when close-event or reconcile work is pending; the sustained-traffic PTY-progress proof covers this.
9. **Reconcile validation race.** A route re-bound with a new generation between slices must not be closed against stale expectations; the exact query returns the live generation and the existing `reconcile_inventory` generation rule decides, with its own test.

## Acceptance checks/tests

Deterministic scheduler state tests (unit):
- Both classes pending: selection alternates Pump/Maintenance across consecutive turns (round-robin proof over a fixed sequence).
- One class pending: that class runs on consecutive turns without requiring the other.
- Marks coalesce: repeated marks produce one pending flag, one slice.
- One turn, one slice: the turn-decision function never returns two background slices for one turn, including when the reconciliation interval is due during a maintenance-pending turn.
- Control precedence: extend `queued_control_precedes_a_due_maintenance_slice` to cover both classes pending.
- No cancellation: a selected slice decision survives a control message that arrives after selection (decision-level proof; negative control with the rule inverted must go red).
- Composed maintenance fairness: with Pump continuously rearmed and Maintenance continuously pending, drive the composed scheduler for a fixed turn sequence and assert that `SubscriberDelivery` and `HostBridge` each execute within an exact finite turn bound. With class round-robin (Maintenance every second background turn) and the 9-kind inner rotation, each kind must execute within 18 background turns; the test asserts the deterministic turn indices, not elapsed time.
- Pump phase work bounds: each Pump phase test proves its per-slice caps (`max_admissions_visited`, `max_candidate_classifications`, `max_route_entries_visited`, `max_routes_validated` as a stream-map visit budget, `OBSERVE_SLICE_BUDGET` items/bytes) and cursor continuation. Admission and stream cursors resume with `BTreeMap::range`. Mux close-event visits count open and reported rows toward the entry budget. A check (test or source assertion) proves the owner loop makes no `list_terminal_subscriptions` and no close-classification `list_sessions` call, and that the control path does not call a full-set close-event helper.
- Status and ReadModeFlags do not mark Pump. A decision-level test covers those reads plus Attach/RemoveSession mark sources.
- CloseEvents behavior matrix: a running-session adapter close still emits `TerminalSubscriptionClosed`; an exited session stays suppressed; a shut-down (stopping/stale/absent) session stays suppressed; a query error stays suppressed. A negative proof shows CloseEvents raises no journal-advanced wake (the registry-state query is non-mutating).
- InventoryReconcile exact-query behavior: absence closes the route; a generation mismatch under the existing rule closes the route; a live matching generation survives; a route re-bound with a newer generation is not closed on stale expectations.
- ReadModeFlags no-observe check: issuing ReadModeFlags moves no observation counter (or the equivalent source-scan assertion).
- Downstream Core dependency proof: the consumed Core pin revision contains both exact queries (Cargo.toml rev at or after the merged Core ticket_1787104273_140454 revision), and the Core-side non-mutating negative test is green in that revision.

Integration proof (`tests/hub_daemon_lifecycle`):
- Sustained control traffic: pipelined Status flood while a live PTY producer emits output. Assert `reconciliation_wakes` strictly increases **and** PTY output progresses (screen/drain content advances) during the flood. Progress deltas, not elapsed time, are the oracle.
- Ready Spawn: with a live-session backlog larger than one observe slice, a Spawn completes successfully; the decision-level oracle proves control precedence and is the deterministic gate. In addition, implementation/Verify evidence **must record one actual measured ready-Spawn observation below 50 ms** from a live run — the ticket requires the observation itself, not an expectation. Per [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]], the elapsed measurement does not become a pass/fail test assertion; a measurement at or above 50 ms triggers investigation and re-measurement, never a budget change.
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
