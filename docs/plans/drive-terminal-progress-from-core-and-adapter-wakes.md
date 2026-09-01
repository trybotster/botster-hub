# Hub: drive terminal progress from Core and adapter wakes

Ticket: `ticket_1787894427_525056`
Run: `run_1788046974_604085`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Repository path of record: the routed `botster-hub` run worktree.
- Base ref: `main`. Verification base commit: `c674a62ac505b990e06f4aca34db1daf586996dc`.
- The target repository comes from the ticket `target_id` through `list_spawn_targets`. It does not come from the process working directory.

## Repository playbook loaded

- [[botster-hub-playbook]] — repository ownership charter for `botster-hub`.

## Other role and surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Class overlay (runtime-teardown class applies, see below):

- [[botster runtime teardown lenses]]

Atomic notes:

- [[core terminal progress is wake driven and targeted]]
- [[core waking terminal adapters shipped at revision ec589ee]]
- [[terminal adapters emit coalesced writable and closed wakes]]
- [[core ingress wake sources are transport neutral]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[code moves need paired absence and presence source guards]]
- [[exact Rust test ablations require a one test baseline]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[count before publish or a concurrent counter cannot be exact]]
- [[session wake coalescing belongs in a lifecycle registry not each handle]]
- [[session ingress wakes retire on observed exit not shutdown acceptance]]
- [[an overflow reconcile walk must reuse the readiness filter it backstops]]

Notes deliberately not loaded: no other repository charter, and no Project Pipelines
package or plugin overlay. This run changes no Project Pipelines path.

## Context loaded

- Vault capture: `ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md`
  (frozen architecture, directory map, migration order step 8, required proof, non-goals).
- Target repository source: `src/daemon/owner_loop.rs`, `src/daemon_maintenance.rs`,
  `src/subscription/attach_routes.rs`, `src/subscription/closed_events.rs`,
  `src/transport/shared/*`, `src/transport/unix/*`, `src/transport/webrtc/*`,
  `src/runtime.rs`, `src/daemon.rs`, `src/lib.rs` source guards.
- Target repository gates: `test.sh`, `docs/hub-resource-proof.md`,
  `docs/lifecycle-suite-harness.md`, `tests/hub_daemon_lifecycle/*`.
- Dependency repository source at the publish revision: `botster-core`
  `origin/main` = `0aed7eb3e09a65e1f2ac9253147d88b3bb4750b0`, including
  `docs/architecture/core-daemon.md` and `docs/architecture/terminal-adapter.md`.
- Closed dependency tickets: `ticket_1787894424_927579` (Core waking adapters) and
  `ticket_1787894965_150479` (Hub decomposition 4b).

### Load-bearing facts established from the code

1. `src/daemon_transport.rs` no longer exists. Decomposition steps 1 through 6 are done.
   The remaining migration step for this ticket is step 8.
2. Today the only production path that pushes terminal frames into a bound Hub adapter is
   `run_pump_observe_phase` -> `HubRuntime::observe_lifecycle_slice` ->
   `CoreDaemon::observe_lifecycle_slice` -> `observe_session` -> `drain_runtime_once`.
   `HubRuntime::drain_runtime_once` and `HubRuntime::drain_subscription` are already
   `#[cfg(test)]`, and `src/lib.rs` forbids both in production sources.
3. At `botster-core` `origin/main`, `observe_lifecycle_slice` **retains** incidental
   terminal egress and **does not pump bound adapters**. `read_screen`,
   `read_mode_flags`, `capture_snapshot`, and `capture_color_and_snapshot` also do not
   pump bound adapters.
4. Therefore the Core pin roll alone would stop all Hub terminal output. The pin roll and
   the wake-driven driver are one cold cut and must land in the same commit range.
5. `CoreDaemon` in `botster-hub` lives behind `HubRuntime.core_daemon: Mutex<CoreDaemon>`
   (`src/runtime.rs:148`) and is referenced only inside `src/runtime.rs`.
6. `CoreDaemon::wake_source()` returns a cloneable `TerminalWakeSource`. `wait_wakes`
   takes `&self`. `pump_woken` takes `&mut self`. A driver thread can therefore block on
   a cloned wake source without holding the Hub `CoreDaemon` mutex.
7. Hub close-event delivery is already off the owner thread: `ClosedEventLedger` is
   `Mutex`-backed, and the Unix `mux_write` task and the WebRTC control channel task pop
   pending events. Only the *classification and queueing* step runs on the owner loop
   today, in `run_close_events_phase`, as a full walk over every admission and route.
8. `should_mark_pump_after_control` already returns `false` for generic control requests.
   Status and ListSessions do not mark the pump. The remaining coupling is that the
   `Observe` pump phase drains terminal output as a side effect at the old pin.

## Scope

In scope:

1. **Core pin roll** to `0aed7eb3e09a65e1f2ac9253147d88b3bb4750b0`.
   - `Cargo.toml` (5 rev literals), `crates/botster-hub-test-support/Cargo.toml` (3),
     `crates/botster-hub-client/Cargo.toml` (1),
     `crates/botster-hub-test-support/build.rs` `PROTOCOL_REV`,
     `crates/botster-hub-test-support/src/conformance_data.rs`
     `LATE_ATTACH_GHOSTSNP_CORE_PIN`, `crates/botster-hub-test-support/src/lib.rs:6433`,
     `tests/session_projection_owner_loop.rs` `REQUIRED_CORE_REV`,
     `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` `LOCKED_CORE_REV`,
     `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`,
     `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`,
     `tests/hub_daemon_lifecycle/event_plane_saturation.rs`,
     `tests/hub_daemon_lifecycle/package_event_plane.rs`, and the six `Cargo.lock` sources.
   - Requirement: zero matches for `7eafa47` outside `docs/plans` and `docs/reports`
     historical documents.
2. **Waking adapter conformance in Hub.** Implement `WakingTerminalAdapter` for
   `UnixTerminalAdapter` and `WebRtcTerminalAdapter` through one shared change in
   `src/transport/shared/adapter_slot.rs`:
   - the slot stores the Core `TerminalWakeSink` installed by `set_wake_sink`;
   - `complete_active` (capacity returned) emits one `Writable` wake;
   - `close` emits one `Closed` wake and stays idempotent;
   - a closed slot never emits a later `Writable` wake.
3. **Waking bind.** Add `HubRuntime::bind_waking_terminal_adapter` and switch
   `bind_unix_adapter_after_attaching` and `bind_webrtc_adapter_after_attaching` in
   `src/subscription/attach_routes.rs` to it. The polling `bind_terminal_adapter` is
   removed from every production Hub path.
4. **New `src/data_plane/driver.rs` (plus `src/data_plane.rs` module root).** One owned
   thread named `botster-hub-data-plane`:
   - clones `TerminalWakeSource` at start;
   - blocks in `wait_wakes(WATCHDOG)` without holding the `CoreDaemon` mutex;
   - locks the `CoreDaemon` mutex only for `pump_woken(&batch, now_seconds)` and releases it;
   - drains a bounded slice of route-specific close work and queues the exact
     `TerminalSubscriptionClosed` events for those routes;
   - exits on an explicit stop flag and is joined before `CoreDaemon::shutdown`.
   `HubRuntime.core_daemon` becomes `Arc<Mutex<CoreDaemon>>`. The `Arc` clone is the only
   Hub state the driver thread receives besides the wake source and the close-work queue.
   There remains exactly one `CoreDaemon`, one owner of it, and one mutex.
5. **Route-specific close-event progress.** Replace the shared
   `PendingRuntimeState.close_work: Arc<AtomicBool>` scan trigger with a bounded
   route-keyed close-work queue:
   - `AdapterSlot::close` pushes `(session_id, subscription_id, generation)` through a
     route hook installed by `UnixConnectionMux::register` / `WebRtcConnectionMux::register`;
   - the hook holds a `Weak` back-reference to its mux, so no `Arc` cycle is created;
   - the driver pops a bounded number of keys per turn, classifies each session with
     `CoreDaemon::session_registry_state` (the existing
     `session_close_event_decision` mapping, unchanged), and queues the closed event on
     that route only;
   - `ClosedEventRoute.reported` continues to provide idempotency.
   `run_close_events_phase` and `PumpPhase::CloseEvents` are removed from the owner loop.
6. **Owner-loop reduction.** `run_pump_observe_phase` keeps bounded lifecycle observation
   and journal-wake handling only. `PumpPhase` reduces to `InventoryReconcile` plus
   `Observe`. Entity reconciliation, inventory reconciliation, package-event delivery,
   maintenance slices, and read-only control work stay on the owner loop unchanged.
7. **Observability.** Add bounded Hub counters for wake-driven progress
   (`data_plane_pumped_routes`, `data_plane_close_routes`, `data_plane_wake_batches`)
   exposed through the existing lifecycle/diagnostic surface, so the proofs below have a
   Hub-visible oracle instead of only byte arrival.
8. **Test seams** gated by `BOTSTER_ENV=test` on the Hub child: stop or pause the driver,
   force adapter `WouldBlock`, and set a long watchdog interval. These carry proofs 2, 3,
   and 4 below.
9. **Source guards.** Add `src/data_plane.rs` and `src/data_plane/driver.rs` to the fixed
   `src/lib.rs` production-scan file list, with one ablation per added file. Add paired
   absence and presence guards for the moved responsibility.
10. Update `docs/hub-resource-proof.md` with the added thread, and add a short
    architecture note for the data-plane driver.

Explicitly out of scope:

- The dedicated DataChannel wire cutover (migration step 9). No wire DTO, serde name, or
  protocol change in this ticket.
- Removing `CoreDaemon::bind_terminal_adapter` from Core, or deleting the Core polling
  path (migration step 10, Core-owned).
- Replay buffers, terminal sequence replay, transport crate extraction.
- Client changes in `botster-web`, `botster-tui`, or `botster-hub-client` DTOs.
- Any further Hub decomposition slice.

## Repository ownership boundaries and cross-repository dependencies

- `botster-core` owns the terminal state machine, subscription identity, generations,
  attach phases, ordering, bounded queues, pressure, fencing, teardown, the wake contract,
  and the targeted pump. This ticket adds no terminal semantics to Hub.
- `botster-hub` owns admission, route policy, concrete Unix and WebRTC adapters, the
  process that hosts them, and the host wait loop that calls the Core pump. Hub stays
  content blind: the driver moves no frame bodies and decodes nothing.
- Concrete transports stay Hub modules. This ticket creates no transport crate.
- Cross-repository prerequisites are already satisfied and closed:
  - `ticket_1787894424_927579` (botster-core, waking adapters and targeted pumping) —
    closed; its output is `botster-core` `origin/main` `0aed7eb`.
  - `ticket_1787894965_150479` (botster-hub decomposition 4b) — closed; `daemon_transport.rs`
    is gone.
  No new dependency ticket is required. If the Implement agent finds that the
  `@trybotster/terminal-protocol` `0.2.0` feature `transport=duplex_binary` forces a
  client-visible negotiation change, that consumer work is registered as a new dependency
  against the client repository target rather than absorbed here.

## Runtime-teardown class

`teardown_class_applies`: **yes**. The ticket changes adapter close paths, route teardown
ownership, multi-subscription isolation, and introduces a new long-lived host thread in
the terminal byte path. Terminal-state versus live-runtime divergence is exactly the
failure this slice can create.

`teardown_isolation`:
- One failed route's ownership set is: the Core route wake state, the Core subscription,
  the Hub `ClosedEventRoute` entry in its mux, the adapter slot, and the mux route handle.
- One route's close must not touch a sibling route, a sibling session, or another
  connection. Core hard-stops only the blocked route after 512 rejected `Writable` pumps
  and preserves sibling routes.
- One failed *connection* still kills every route on that connection through the existing
  `close_all` path. That is unchanged and intentional: those routes share the socket.

`teardown_bounds`:
- The driver never blocks on transport I/O. `wait_wakes` takes a watchdog timeout; the
  timeout is a hang bound, not a progress mechanism.
- The driver holds the `CoreDaemon` mutex only across one `pump_woken` call and one
  bounded close slice. It never holds the mutex across `wait_wakes`.
- Close work per turn is bounded by an explicit constant, reusing the existing
  `PUMP_MAX_*` budget style.
- Hard stop: an `AtomicBool` stop flag plus a wake pushed by the stopper ends the loop
  even if the wake source is quiet. `HubRuntime::release_for_restart` sets the flag, wakes
  the source, and joins the thread with a bound before Core shutdown runs. A join timeout
  is a fail-closed diagnostic, not a silent leak.

`late_message_matrix`:

| Message or event | Owner tag | Rejection after terminal failure | Residual sweep |
|---|---|---|---|
| `Attach` (creates route, then bind) | `(client_id, session_id, subscription_id, generation)` from the Core live inventory before bind | `live_generation_for_route` returns `None`, then `fail_closed_pre_bind_attach` | existing pre-bind fail-closed path, unchanged |
| Waking bind | same key, plus Core rejection ladder before wake state allocation | Core returns `BindTerminalAdapterError`; Hub closes the handle | unchanged; wake state is allocated only after rejection checks pass |
| `Writable` wake after close | route wake state, retired on hard stop | closed slot must not emit `Writable`; Core drops wakes for retired routes | driver pump is a no-op on a route Core no longer holds |
| `Closed` wake / close-work key | `(session_id, subscription_id, generation)` | `ClosedEventRoute.reported` and the suppression set reject a second event | bounded close slice; a key whose mux `Weak` is dead is dropped |
| `Detach`, `ShutdownSession`, `RemoveSession` | exact `(session_id, subscription_id, generation)` suppression, existing behavior | existing exact-key suppression before Core shutdown | unchanged; no session-wide suppression |
| `SubscribeEntities` / package-event subscribe | connection-scoped, owner loop | unchanged | unchanged; these never enter the driver |
| Ingress wake for a stopping session | `SessionId` in the Core live-session registry | wake stays live through `Stopping`; `ProcessExited` or runtime removal retires it | Core `forget_session` after teardown commits |

`production_path_proof`:
- Exact path: PTY or worker output -> Core ingress wake -> `TerminalWakeSource` ->
  `DataPlaneDriver::wait_wakes` -> `CoreDaemon::pump_woken` -> `try_write` on the exact
  bound adapter -> `AdapterSlot` -> Unix `mux_write` task or WebRTC subscription channel
  -> client bytes.
- Close path: adapter close -> `AdapterSlot::close` -> `Closed` wake to Core **and**
  route key to the close-work queue -> driver -> route-specific
  `queue_closed_subscription_events_bounded` -> ledger -> transport writer ->
  `TerminalSubscriptionClosed`.
- Oracles are the live `tests/hub_daemon_lifecycle` suite against the real `botster-hub`
  binary and the real session worker, not unit calls. Every proof below names a
  red-on-revert control.

`ownership_identity`:
- Every durable route row keeps the existing stable owner identity
  `(client_id, session_id, subscription_id, generation)`.
- The close-work key carries the generation, so a delayed close key cannot delete or
  report a row now owned by a replacement subscription with a reused `subscription_id`.
- Both queue orders are covered: "closed first, then replacement binds" and "replacement
  binds first, then the stale close key arrives".

`sibling_fail_closed_policy`:
- On successful route close: siblings on the same connection, the same session, and other
  connections keep working. Required test.
- On ultimate close failure of one adapter: Core hard-stops that route after its bounded
  rejected-pump budget and preserves siblings. Hub emits the core-adapter closed event for
  that route only.
- The existing local-WebRTC ultimate close failure policy (sibling sacrifice on the
  dedicated runtime) is unchanged by this ticket and is not weakened.

## Assumptions and unknowns

Assumptions, stated explicitly:

1. The published waking revision to pin is `botster-core` `origin/main`
   `0aed7eb3e09a65e1f2ac9253147d88b3bb4750b0`. It contains `ec589ee` plus the `af15dbd`
   count-before-publish fix and the quiescence oracle. Pinning `ec589ee` itself would
   import a known inexact occupancy counter.
2. `observe_lifecycle_slice` at the new pin does not pump bound adapters. This is asserted
   in `botster-core` `docs/architecture/core-daemon.md`. Implement must confirm it by test,
   not by reading the document alone.
3. Starting the driver inside `HubRuntime` construction (not only inside `serve_daemon`)
   is the correct owner, because the in-process lifecycle harness and the daemon must take
   the same production path. Implement may move it to `serve_daemon` only with evidence
   that no in-process production path needs it.
4. `MAX_OWNER_TURN_MS = 25` and the 64-thread Hub bound stay unchanged. One added thread
   is within budget.

Unknowns for Implement to resolve before finishing:

- **U1.** Does `@trybotster/terminal-protocol` `0.2.0` (`transport=duplex_binary`,
  `conformance_fixture_revision` 1 -> 2) change the Hub-advertised terminal compatibility
  feature set or the default requirement? `hello_ack_advertises_independent_terminal_compatibility`
  and `unix_adapter_feature_does_not_raise_default_requirement` are the tripwires. If the
  default requirement would rise, stop and ask a human: that is a client-visible change
  and is out of this ticket's scope.
- **U2.** Whether `node packages/hub-test-support/scripts/sync-assets.mjs --check` requires
  a regenerated asset set after the pin roll.
- **U3.** Whether any existing in-crate test binds an adapter and depends on
  `observe_lifecycle_slice` for byte progress. Those tests must move to the driver or to
  the `#[cfg(test)]` drain helper, and each conversion must keep its original assertion.
- **U4.** Contention shape between the driver and the owner loop on the `CoreDaemon`
  mutex under the event-plane saturation campaign. If owner-turn latency regresses past
  `MAX_OWNER_TURN_MS`, report it rather than raising the bound.

## Affected surfaces and files

New:

- `src/data_plane.rs`
- `src/data_plane/driver.rs`

Changed:

- `Cargo.toml`, `Cargo.lock`
- `crates/botster-hub-client/Cargo.toml`
- `crates/botster-hub-test-support/Cargo.toml`, `build.rs`, `src/lib.rs`,
  `src/conformance_data.rs`
- `src/lib.rs` (module registration, production-scan file list, guards)
- `src/runtime.rs` (`Arc<Mutex<CoreDaemon>>`, waking bind, wake-source and shared-daemon
  accessors, driver ownership and stop in `release_for_restart`)
- `src/daemon/owner_loop.rs` (remove the close-events phase and the terminal side of the
  observe phase; remove `PendingRuntimeState.close_work`)
- `src/daemon_maintenance.rs` (`PumpPhase` rotation, close-slice budgets that move)
- `src/subscription/attach_routes.rs` (waking bind on both routes)
- `src/subscription/closed_events.rs` (route-specific queueing entry point; owner-loop
  phase removed)
- `src/transport/shared/adapter_slot.rs`, `src/transport/shared/wake.rs`
- `src/transport/unix/adapter.rs`, `src/transport/webrtc/adapter.rs` (implement
  `WakingTerminalAdapter`, install the route close hook at `register`)
- `docs/hub-resource-proof.md`
- `tests/hub_daemon_lifecycle/*` (pin literals plus the new proofs)
- `tests/session_projection_owner_loop.rs`,
  `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` (pin literals)

## Risks

1. **Cold cut.** The pin roll removes owner-loop terminal pumping in Core while the driver
   supplies the replacement. A partial landing produces a Hub that attaches but never
   delivers bytes. Mitigation: one commit range, and proof 1 is the gate.
2. **Mutex contention.** The driver and the owner loop now share the `CoreDaemon` mutex.
   A long owner turn can delay terminal bytes and a hot terminal can delay control
   responses. Mitigation: hold the lock only across `pump_woken`, and prove both
   directions (proofs 1 and 10).
3. **Arc cycle or leak in the close hook.** A strong back-reference from the adapter slot
   to its mux leaks every connection. Mitigation: `Weak` back-reference plus the existing
   resource-bound proof.
4. **Wake loss on close.** If `close` emits the `Closed` wake before the route key reaches
   the close-work queue, or the queue is full, a route can close without a client event.
   Mitigation: bounded queue with an overflow flag that forces one reconciliation slice,
   mirroring the Core overflow arm; proof 5 includes an overflow arm.
5. **Double close event.** Two close sources (host close and transport death) can push two
   keys. Mitigation: `reported` idempotency plus an explicit repeated-close proof.
6. **Test churn hides a real regression.** Many existing tests were written against the
   polling path. Mitigation: every converted test keeps its original assertion text and
   subject; conversions are reviewed as behavior-preserving.
7. **Protocol 0.2.0 side effects** (U1). Mitigation: named tripwire tests; ask a human
   before changing an advertised default requirement.
8. **Shutdown race.** `CoreDaemon::shutdown` itself waits on `wait_wakes` plus
   `pump_woken`. A live driver competing for the same source can starve shutdown.
   Mitigation: stop and join the driver before Core shutdown; proof 9.
9. **Resource bound.** One added OS thread must not break the 64-thread invariant or the
   idle CPU bound. The driver must not spin: an empty batch must block, not loop.

## Acceptance checks and tests

Every live proof runs against the real `botster-hub` binary and the real session worker in
the `hub_daemon_lifecycle` suite. Every proof names its red-on-revert control. Exact
Cargo filters use the full module path and show a one-test baseline before the ablation.

1. **Terminal bytes progress while the owner loop is idle.** Attach over Unix, then emit
   PTY output while no control traffic and no owner background slice runs. Assert the bytes
   arrive and that `reconciliation_wakes` and `lifecycle_session_drains` do not advance
   across the arrival window, while `data_plane_pumped_routes` does advance.
   Red-on-revert: remove the driver start; the test must fail with no bytes.
2. **Generic control does not drive terminal progress.** With the driver paused through
   the `BOTSTER_ENV=test` seam, issue `Status`, `ListSessions`, `ReadScreen`,
   `ReadModeFlags`, `CaptureSnapshot`, `ModeGatedInput`, and a shutdown classification
   request. Assert zero terminal frames reach the adapter. Resume the driver and assert the
   retained frames then arrive in order.
   Red-on-revert: restore an owner-loop terminal pump; the zero-frame assertion must fail.
3. **A full adapter resumes from its writable wake.** Force adapter `WouldBlock`, let Core
   retain the frame, then clear pressure. Assert exactly one coalesced `Writable` wake and
   resumed delivery, with the watchdog interval set long enough that a timer cannot explain
   the progress.
   Red-on-revert: drop the `Writable` emission in `complete_active`.
4. **Ingress progresses without a correctness timer.** Set the watchdog interval to a value
   far above the test deadline. Assert worker and PTY output still arrive.
   Red-on-revert: replace the ingress wake with the timer.
5. **Close is route-specific and idempotent.** Close one route on a connection holding
   several routes. Assert exactly one `TerminalSubscriptionClosed` for the exact
   `(session_id, subscription_id, generation)`, no event for a sibling route, and no second
   event on a repeated close. Include an overflow arm that fills the close-work queue and
   still delivers every route's event exactly once.
   Red-on-revert: return to the admission-wide close scan; the sibling-silence and
   exactly-once assertions must fail.
6. **One stalled subscription does not block a sibling.** Jam one adapter `Full` while a
   sibling subscription on the same session and connection keeps receiving output. Then
   exhaust the Core rejected-pump budget and assert the blocked route hard-stops while the
   sibling survives and still delivers.
7. **Attach-frame retention re-verified through the waking bind (`c25368e`).** Declare an
   attach before the host binds, then bind the waking adapter. Assert the production order
   `Attaching` -> `Snapshot` (when non-empty) -> `Attached` -> `TerminalOutput` with no
   dropped or duplicated frame. This is a new proof because the current pin predates
   `c25368e`.
8. **Stale generations cannot bind, wake, close, or replace a live route.** Extend the
   existing replacement-owner proofs to the waking bind and to a stale close-work key.
9. **Shutdown ordering.** Assert the driver stops and joins before `CoreDaemon::shutdown`,
   that exact `(session_id, subscription_id, generation)` suppression still holds for
   admitted Unix and WebRTC routes before Core shutdown, and that no Hub thread, session
   worker, zombie, or socket survives an orderly down.
10. **Terminal traffic does not delay control responses.** Under sustained terminal output,
    assert control response latency and `max_owner_turn_us` stay within
    `MAX_OWNER_TURN_MS`.
11. **Resource bounds.** `focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload`
    plus `docs/hub-resource-proof.md` updated: exactly one added `botster-hub-data-plane`
    thread, at most 64 Hub OS threads, unchanged queue capacities, counters returning to
    zero, and the idle CPU bound unchanged (the driver must block, not spin).
12. **Source guards.** `src/data_plane.rs` and `src/data_plane/driver.rs` are added to the
    fixed production-scan list, with one ablation per added file. Paired guards: absence of
    any terminal pump call in `src/daemon/owner_loop.rs` and
    `src/subscription/closed_events.rs`, and presence of `pump_woken(` in exactly one
    production file, `src/data_plane/driver.rs`.
13. **Pin-roll completeness.** Zero matches for `7eafa47` outside `docs/plans` and
    `docs/reports`; all six `Cargo.lock` sources rolled; every active revision literal
    rolled.

Gate commands (record `rustc --version` from the same shell):

```sh
RUSTUP_TOOLCHAIN=1.97.0 cargo fmt --all -- --check
RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings
# unset CARGO_TARGET_DIR; prebuild into the default worktree target/ first
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked -p botster-core-daemon --bin botster-session-worker
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --bin botster-hub
RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked
(cd packages/hub-test-support && npm install --no-save && npm test)
```

Gate hygiene: the worktree path must be colon-free and `CARGO_TARGET_DIR` must be unset for
the official `./test.sh --locked` gate. The lifecycle suite needs a quiet host; poll for a
quiet window rather than accepting `environment_dirty`.

## Vault gaps worth capturing

1. **Hub adopts the waking Core data plane through one owned driver thread** — the shape
   chosen here (cloned wake source outside the mutex, `pump_woken` inside it, stop-and-join
   before Core shutdown) is a durable Hub host contract and is not yet a note.
2. **`observe_lifecycle_slice` retains terminal egress and does not pump bound adapters at
   the published revision** — this is the fact that makes the pin roll a cold cut. It is
   documented in the Core repository but not in the vault.
3. **Route-keyed close work replaces admission-wide close scanning** — the generation-tagged
   close key and its `Weak` mux back-reference are the anti-leak and anti-stale-teardown
   rule for future Hub route work.
4. **Pin the published waking revision `0aed7eb`, not `ec589ee`** — the existing note warns
   that the `ec589ee` occupancy counter is inexact; the consumable pin should be named.
