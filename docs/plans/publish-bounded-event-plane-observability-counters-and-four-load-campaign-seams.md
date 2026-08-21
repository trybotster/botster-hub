# Plan — Hub: publish bounded event-plane observability counters and four load-campaign seams

- Ticket: `ticket_1787267568_492780`
- Run: `run_1787278338_832165`
- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` at `b3b54f1` ("Merge ticket: Roll Core pin after IncrementalAttach local-runtime gate")
- Core pin (verified in `Cargo.toml:24-26,43-44`): `7eafa470a18025895995bbedc20d34b58106a03b`

## 1. Repository routing

I resolved the run `target_id` through `list_spawn_targets`. `tgt_7e208a0c76a44980a83b63af976b1f22` is
`botster-hub` at `/Users/jasonconigliari/Projects/botster-hub`, repo `trybotster/botster-hub`. I did not
infer the repository from the process working directory.

Repository playbook loaded: `[[botster-hub-playbook]]`.

## 2. Playbooks and atomic notes loaded

Role playbooks, in order:

1. `[[planner-playbook]]`
2. `[[botster-planner-playbook]]`
3. `[[botster-hub-playbook]]` (repository ownership charter)

Targeted atomic notes:

- `[[load diagnostics must not cost work proportional to what they measure]]`
- `[[saturation counters do not acquire the contended lock they report]]`
- `[[Hub event plane lacks seven load campaign signals]]`
- `[[package event handler timeouts are discarded as successful completions]]`
- `[[spawned Hub tests can reach only four of fourteen Core test builders]]`
- `[[hub client event queue max requires Botster test mode]]`
- `[[test names do not prove their bodies can fail on the named claim]]`
- `[[router ingress uses try_lock only and contention is shed_busy]]`
- `[[botster hub events use bounded priority lanes instead of unbounded queue fuses]]`
- `[[botster hub is a first party host profile over core]]`
- `[[botster data plane bypasses the hub through session and client actors]]`
- `[[Hub suite runs prebuild the session worker before the locked test wrapper]]`
- `[[vault example paths are not repository placement conventions]]`

`[[project-pipelines-playbook]]` is **not** loaded. No Project Pipelines package or plugin path is in scope.

## 3. Runtime-teardown class

`teardown_class_applies: false`.

The change adds counters, an internal `ControlMessage` field, status fields, and four test-mode
configuration reads. It does not change WebRTC or peer lifecycle, `SessionIo`/`ClientWorker` teardown,
multi-peer ownership, resource-spin behavior, or terminal-state versus live-runtime divergence. Scope
item 8 forbids any scheduling or budget change, so no teardown decision moves. I therefore did not load
`[[botster runtime teardown lenses]]`, per the explicit instruction not to apply it outside its class.

## 4. Context loaded (code read at base `b3b54f1`)

- `src/package_event_router.rs`: `EventPlaneStatus` (`:29-42`), `EventPlaneSnapshot` (`:163-172`),
  `RouterInner` and `PackageEventRouter` (`:213-236`), the pull/expiry loop (`:569-660`), and
  `snapshot()` (`:800-834`).
- `src/daemon_event_subscriptions.rs`: `QueuedClientEvent` and `ClientGapSlot` (`:110-142`),
  mailbox overflow gap (`:183-185`), `mark_gap` (`:230-243`), mailbox age expiry (`:264-270`),
  `test_client_event_queue_max_from` and its negative test (`:550`, `:913-924`).
- `src/daemon_maintenance.rs`: `MAX_OWNER_TURN_MS = 25` and `MAX_READY_OPERATION_WAIT_MS = 50` (`:34-36`),
  `MaintenanceState.last_owner_turn` (`:619`), the two `timeout_ms: 1_000` admissions (`:1121`, `:1274`),
  and `run_completion_drain_slice` (`:1319-1345`).
- `src/daemon_transport.rs`: owner poll and slice loop (`:339-370`), owner-turn write (`:293`),
  the two `ControlMessage::EgressWriteFailed` senders (`:776`, `:1896`), the write-deadline error
  (`:910-913`), `ControlMessage` (`:5296-5345`), and `record_egress_write_failure` (`:3072-3079`).
- `src/runtime.rs`: `core_daemon_config` (`:4612-4641`), the `cfg(test)` journal-capacity helper
  (`:4590-4610`), and `take_journal_advanced_wake` (`:3183-3189`).
- `src/lua_runtime.rs`: `DEFAULT_INSTRUCTION_BUDGET = 500_000` (`:55`), the instruction hook (`:553-566`),
  and `LuaPluginRuntime::invoke` (`:591-640`).
- `crates/botster-hub-client/src/lib.rs`: `PROTOCOL_VERSION = 7` (`:30`),
  `CONFORMANCE_FIXTURE_REVISION = 44` (`:31`), `DaemonStatus` (`:2342-2367`), and
  `DaemonLifecycleCounters` (`:2472-2502`, `stalled_writes` at `:2495`).
- `README.md:453` and `git log` confirm `docs/plans/**` as the plan destination in this repository.

I independently reproduced the ticket's absence claim: `grep -rni "latency" src/ crates/` returns comment
hits only, and `stalled_writes` is written at exactly one site (`src/daemon_transport.rs:3078`).

## 5. Scope

### In scope

**S1. New bounded counter module `src/event_plane_counters.rs`.**
One `EventPlaneCounters` struct, owned by `HubRuntime` as an `Arc`, stored **beside** `PackageEventRouter`
and never inside `RouterInner`. Contents:

- `shed_by_reason`: a fixed `[AtomicU64; 12]` array indexed by an `EventPlaneStatus::index()` `const fn`.
  Every non-`Accepted` ingress status is counted, including `ShedFull` and `ShedBusy`. Fixed array, so no
  map growth and no allocation per event.
- `admission_attempts`, `delivery_attempts`: `AtomicU64`.
- `admission_latency`, `delivery_latency`: fixed-bucket histograms. Each is
  `{ buckets: [AtomicU64; 13], count: AtomicU64, sum_us: AtomicU64, max_us: AtomicU64 }`. The bucket index
  is computed with `u64::leading_zeros` — one arithmetic step, no loop and no scan, per
  `[[load diagnostics must not cost work proportional to what they measure]]`.
  - **admission latency** is the wall time from entry of the router `try_emit` ingress call to the
    returned `EventPlaneStatus`.
  - **delivery latency** is the wall time from `Envelope.enqueued_at` to the moment a queued copy becomes
    a `ReadyDelivery` in the pull loop.
- Typed timeout counters T1–T3 (see S3).
- Oldest-age cells: three `RwLock<BTreeMap<String, Arc<AgeCell>>>` maps, one for producer queues, one for
  consumer queues, and one for client mailboxes. `AgeCell` is one `AtomicU64` holding nanoseconds since a
  `base: Instant` captured at construction, with `u64::MAX` as the empty sentinel. Writers update the cell
  in O(1) at push and at pop, using the queue head that is already in hand. The map takes a write lock only
  on first registration of an owner, plugin key, or connection.

**S2. Saturation-safe read path (ticket item 6).**
A new `HubRuntime::event_plane_counters_snapshot()` reads only atomics plus short read guards on the
counters' own maps. It never touches `PackageEventRouter::inner`. `PackageEventRouter::snapshot` keeps its
`try_lock` behavior unchanged for ordinary inspection, per
`[[saturation counters do not acquire the contended lock they report]]` and
`[[router ingress uses try_lock only and contention is shed_busy]]`.

**S3. Four distinct timeout counters.**

- **T1 — package-event handler invocation timeout.** `run_completion_drain_slice`
  (`src/daemon_maintenance.rs:1322-1334`) currently destructures `completion.result` only to read
  `request_id`. Change it to keep the discriminant, and when the request id resolves to an entry in
  `state.event_in_flight`, count by typed `PluginInvocationFailureKind`
  (`TimedOut`, `HandlerFailed`, `Cancelled`, `Backpressured`, `WorkerStopped`) plus a `completed_ok`
  counter. Retirement behavior is byte-for-byte unchanged; only observation is added. Core owns the typed
  kind at `crates/botster-core/src/contract/actor.rs:1233`; Hub is the reporter.
- **T2 — router queue-age expiry.** Increment at the `if expired { retire_holder_locked(...) }` branch in
  the pull loop (`src/package_event_router.rs:595-623`), which drops the envelope silently today.
- **T3 — client-mailbox queue-age expiry.** Increment a counter distinct from overflow at
  `src/daemon_event_subscriptions.rs:264-270`, and increment an overflow counter at `:183-185`. The gap
  bit and `DaemonEvent::EventGap` behavior stay exactly as they are; only the cause becomes countable.
- **T4 — transport write timeout.** Add an internal `EgressWriteClass { Timeout, Other }`. Classify from
  the `error` value already in scope at `src/daemon_transport.rs:774` and `:1894`
  (`DaemonTransportError::Io(e)` with `e.kind() == std::io::ErrorKind::TimedOut`, produced at `:910-913`).
  Carry that one field on `ControlMessage::EgressWriteFailed` beside `delivery_kind`. In
  `record_egress_write_failure` (`:3072`), keep `stalled_writes` incrementing for every write failure and
  increment a new `stalled_write_timeouts` only for `Timeout`. `ControlMessage` is internal, so this needs
  no `PROTOCOL_VERSION` and no conformance fixture revision bump.

**S4. Oldest queue age as a value (ticket item 2).** Publish the age number for each producer queue, each
consumer queue, and each client mailbox from the S1 age cells. The age predicate at
`src/package_event_router.rs:599` and `src/daemon_event_subscriptions.rs:266` is unchanged.

**S5. Owner turn and ready-operation wait (ticket item 5).**

- `last_owner_turn` is already computed at `src/daemon_transport.rs:293`, in a function that also owns
  `state.lifecycle_counters`. Write `last_owner_turn_us` and a `max_owner_turn_us` high-water value there.
  No change to the private `daemon_maintenance` module boundary is required.
- Ready-operation wait becomes a real production measurement: add `enqueued_at: Instant` to
  `ControlMessage::Request`, set it at the two senders (`src/daemon_transport.rs:750` and `:5236`), and
  record `enqueued_at.elapsed()` where the owner loop serves that message (`:2552`). Report count, sum,
  max, and fixed buckets. `ControlMessage` is internal, so no public vocabulary changes.

**S6. Public exposure through the existing status path (ticket item 5, acceptance line 1).**

- Add `stalled_write_timeouts`, `last_owner_turn_us`, `max_owner_turn_us`, and the ready-operation-wait
  summary to `DaemonLifecycleCounters`, each `#[serde(default)]`. That struct already rides `DaemonStatus`
  and already carries `stalled_writes`, so T4 lands beside the total it subsets.
- Add one new `DaemonEventPlaneCounters` struct on `DaemonStatus`, behind
  `#[serde(default, skip_serializing_if = ...)]`, carrying the S1 event-plane values. Grouping them keeps
  `DaemonLifecycleCounters` from absorbing roughly thirty unrelated fields, which the Hub charter's
  gravity warning argues against.
- Both additions are additive and default-tolerant, so `PROTOCOL_VERSION` stays `7` and
  `CONFORMANCE_FIXTURE_REVISION` stays `44`. `DaemonRequest::Status` is unchanged.

**S7. Four `BOTSTER_ENV=test` gated seams (ticket item 7).** One `hub_test_seams()` reader placed beside
`core_daemon_config` in `src/runtime.rs`, in the `BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY` style at
`:4618`. Each value has a pure `*_from(env, raw)` helper so a negative test can prove inertness in the
style of `client_event_queue_max_override_requires_test_mode`
(`src/daemon_event_subscriptions.rs:914`).

| Seam | Variable | Effect | Bound |
| --- | --- | --- | --- |
| 1 | `BOTSTER_HUB_TEST_DROP_JOURNAL_WAKES` | `HubRuntime::take_journal_advanced_wake` (`src/runtime.rs:3184`) takes the Core bit and discards it while a remaining-count atomic is above zero | count clamped to 64 |
| 2 | `BOTSTER_HUB_TEST_LIFECYCLE_JOURNAL_CAPACITY` | `CoreDaemonConfig::with_lifecycle_journal_capacity` in `core_daemon_config`, reachable from a spawned daemon | positive integer |
| 3 | `BOTSTER_HUB_TEST_EVENT_INVOCATION_TIMEOUT_MS` | replaces the `timeout_ms: 1_000` literal at `src/daemon_maintenance.rs:1121` and `:1274` | clamped to 1..=10_000 |
| 4 | `BOTSTER_HUB_TEST_EVENT_HANDLER_HOLD_MS` | `LuaPluginRuntime::invoke` (`src/lua_runtime.rs:591`) holds before calling the Lua function when `request.context.origin == Some("package-event")` | clamped to 0..=5_000 |

Seam 4 exists because `DEFAULT_INSTRUCTION_BUDGET = 500_000` (`src/lua_runtime.rs:55`) aborts
`examples/event-plane-consumer` long before 1000 ms, so no handler can currently time out. Holding in the
Rust runtime before entering Lua avoids the instruction budget entirely and leaves the budget untouched.
Seam 2 removes the `#[cfg(test)]` unreachability recorded in
`[[spawned Hub tests can reach only four of fourteen Core test builders]]`; the existing thread-local
`cfg(test)` helper stays for current in-crate callers.

### Explicitly out of scope

- The saturation campaign itself. That is consumer `ticket_1786663585_879846`.
- Any production budget, queue bound, or scheduling decision (ticket item 8). `MAX_OWNER_TURN_MS`,
  `MAX_READY_OPERATION_WAIT_MS`, `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, `PUMP_MAX_*`,
  `EVENT_DELIVERY_*`, `SESSION_DELIVERY_*`, `DEFAULT_INSTRUCTION_BUDGET`, and every
  `PackageEventPlaneOptions` default stay exactly as they are.
- Hub terminal body access, Workspaces policy, and package product policy (ticket item 9).
- `PROTOCOL_VERSION` bump, conformance fixture revision bump, new transport, and new request vocabulary.
- Any Core source change. Core already publishes every typed kind this plan reads.
- Any change to retirement, gap delivery, shed decisions, or completion semantics. Observation only.

## 6. Repository ownership boundaries and cross-repository dependencies

- **Hub owns** T2, T3, T4, the counters module, the status projection, the owner-turn and
  ready-operation-wait measurements, and all four test seams. All are host-profile policy and
  control-plane observation, which `[[botster hub is a first party host profile over core]]` assigns to Hub.
- **Core owns** the authoritative T1 signal: `PluginInvocationFailureKind::TimedOut`
  (`crates/botster-core/src/contract/actor.rs:1233`), produced by the deadline waiter at
  `crates/botster-core/src/engine/plugin_worker.rs:2331-2421`. Hub only reads a discriminant it already
  receives. **No Core change is required, so this run registers no Core dependency ticket.** The Core pin
  stays at `7eafa470a18025895995bbedc20d34b58106a03b`.
- **`crates/botster-hub-client` is an in-repository workspace member** (`Cargo.toml:4,20`), not a separate
  target in this Hub's spawn-target list. The DTO change is therefore Hub-owned and needs no cross-repo
  ticket. Both DTO additions are `#[serde(default)]`, so `botster-web` and `botster-tui` consumers keep
  deserializing unchanged. No consumer ticket is required.
- **Data plane untouched.** No terminal bytes, scrollback, or per-client egress payload is inspected, per
  `[[botster data plane bypasses the hub through session and client actors]]`. T4 classifies an
  `io::ErrorKind` only; it never reads the frame body.
- **Consumer dependency.** `ticket_1786663585_879846` consumes this surface. The parent Plan Review
  `review_1787278015` states that this ticket's dependency edge must be restored before that ticket's
  Implement step. Restoring that edge is the parent run's action, not this run's, so this plan does not
  add or remove any dependency edge.

## 7. Assumptions and unknowns

**A1 (must be adjudicated by Plan Review).** The ticket says: "Do not start this ticket until Plan Review
approves the revised plan for `ticket_1786663585_879846`." That parent plan is **not yet approved**. Its
run `run_1787262311_549251` returned to Plan at step 11 after review `review_1787278015` required changes.
I planned anyway, on this evidence: that same review states "Hub observability `ticket_1787267568_492780`
remains open and logically required" and that "the numeric budgets, saturation workload, observability
dependency, teardown checks, and failure rules can remain". The rejection targets the parent's
product-specific shared-session Project Pipelines acceptance chain, not this split. The orchestrator also
created this run (1787278338) after that review (1787278015). **I am flagging this rather than waiving it
silently.** If Plan Review disagrees, the correct response is to hold this ticket at Plan, not to change
its scope.

**A2.** Admission latency and delivery latency have no existing definition in this repository. I define
them in S1. If the campaign needs different boundaries, that must be settled at Plan Review, because the
measurement points are hard to move after Implement.

**A3.** I assume Core's deadline waiter returns `TimedOut` for a Background invocation whose runtime
thread is still inside `LuaPluginRuntime::invoke`, so seam 4 can produce T1. Implement must prove this with
the red-first test in AC2 before building anything on top of it. If Core instead waits for the runtime to
return, seam 4 needs a different hold point and Implement must report that finding rather than reshape the
counter.

**A4 (unknown to resolve in Implement).** I have not yet confirmed whether any golden fixture in
`crates/botster-hub-test-support` asserts an exact `DaemonStatus` or `DaemonLifecycleCounters` JSON shape
that an added field would break. `crates/botster-hub-test-support/fixtures` holds only
`plugin-contract-matrix`, and the conformance revision is asserted as a number
(`crates/botster-hub-test-support/src/lib.rs:5480`, `:6110`), which suggests additive fields are safe.
Implement must check before claiming that `CONFORMANCE_FIXTURE_REVISION` stays `44`. If a fixture does pin
the shape, Implement must report it rather than bump the revision silently, because the ticket forbids a
revision bump when the existing path can carry the fields.

**A5.** Seam 4 holds the per-plugin Lua mutex for its bounded duration, so it serializes other invocations
of the same plugin while held. That is acceptable for a test-only, clamped, `BOTSTER_ENV=test` seam, and it
is inert in production.

**A6.** `MaintenanceState.last_owner_turn` stays where it is. Surfacing it needs no module-visibility
change, because `src/daemon_transport.rs:293` already holds both the duration and
`state.lifecycle_counters`. The ticket's framing ("the private module never enters `DaemonStatus`")
describes the symptom; the smaller fix is at the existing write site.

**A7.** The four seams are read at Hub-child startup. Seams 1, 3, and 4 configure Hub behavior, not
`CoreDaemonConfig`, so they are stored on Hub state while being read in one place beside
`core_daemon_config`, in the style the ticket names. Only seam 2 sets a `CoreDaemonConfig` builder.

## 8. Affected surfaces and files

| File | Change |
| --- | --- |
| `src/event_plane_counters.rs` (new) | `EventPlaneCounters`, fixed histogram, age cells, snapshot type |
| `src/lib.rs` | register the new module and re-export the snapshot type |
| `src/package_event_router.rs` | shed-by-reason, admission and delivery attempts, latencies, T2, producer and consumer age cells |
| `src/daemon_event_subscriptions.rs` | overflow gap count, T3 mailbox-expiry count, mailbox age cell |
| `src/daemon_maintenance.rs` | T1 typed completion counting; seam 3 for the two `timeout_ms` sites |
| `src/daemon_transport.rs` | `EgressWriteClass` on `ControlMessage::EgressWriteFailed`, T4 in `record_egress_write_failure`, `enqueued_at` on `ControlMessage::Request`, owner-turn and ready-wait recording, status projection |
| `src/runtime.rs` | `hub_test_seams()` and the four gated reads; seam 1 in `take_journal_advanced_wake`; counters accessor |
| `src/lua_runtime.rs` | seam 4 hold before handler invocation |
| `src/client_api.rs` | carry counters from `HubRuntime` to the client-API status body |
| `crates/botster-hub-client/src/lib.rs` | `DaemonEventPlaneCounters`; new `DaemonLifecycleCounters` fields |
| `docs/plans/...` (this file), `docs/reports/...` (Implement) | plan and report artifacts |
| `README.md` | status-surface documentation for the new fields, if the README documents that surface |

## 9. Risks

- **R1. Observer changes the load class.** Mitigated by fixed arrays, `leading_zeros` bucket selection,
  no allocation on any per-event path, and the AC6 work-bound test.
- **R2. A second lock replaces the first.** The age maps carry their own `RwLock`. Writers hold it for one
  atomic store, and readers never touch the router mutex. AC4 proves the read path returns values while
  the router lock is held.
- **R3. Silent DTO break.** Mitigated by `#[serde(default)]` on every added field and by A4's fixture check.
- **R4. Accidental behavior change.** T1 must keep `run_completion_drain_slice` retirement identical, and
  T3 must not change gap-bit or `EventGap` semantics. Reviewer instruction: diff these two functions for
  control-flow change, not only for added lines.
- **R5. Seam leakage into production.** Mitigated by AC5's four negative tests and by clamping every value.
- **R6. A6 could be wrong about where the owner turn is observable.** If the write site does not have
  `lifecycle_counters` in scope after the change, Implement must report it before widening module
  visibility.

## 10. Acceptance checks and tests

Every ticket acceptance line maps to a check. AC2, AC3, AC4, and AC6 are red-first: Implement must record
the failing output **before** the change, per
`[[test names do not prove their bodies can fail on the named claim]]`.

- **AC1 — public readability.** One test drives `DaemonRequest::Status` through the production daemon path
  and asserts every signal is present and non-absent: queue count and bytes, oldest age per producer queue,
  per consumer queue, and per client mailbox, admission latency, delivery latency, shed by typed reason,
  gap, resync, pressure, T1 through T4 as four distinct values, owner-turn duration, and
  ready-operation wait.
- **AC2 — T1, red first.** A focused test uses seams 3 and 4 to make a package-event handler exceed
  `timeout_ms`, then asserts the `TimedOut` counter incremented by exactly one and every other
  `PluginInvocationFailureKind` counter stayed at zero. A second case makes a handler fail without a
  timeout and asserts `HandlerFailed` incremented instead. Both cases must be shown red at base, where the
  two outcomes are indistinguishable.
- **AC3 — T4, red first.** A focused test proves a write-deadline failure increments
  `stalled_write_timeouts` while a non-timeout write failure does not, and that `stalled_writes` counts
  both.
- **AC4 — saturation read path, red first.** A test holds the router inner lock through
  `PackageEventRouter::test_with_inner_held` and asserts the counter read returns values. The same test
  asserts that `PackageEventRouter::snapshot()` returns `ShedBusy` under that hold, which documents the
  reason the counter path is separate. A `try_lock` based counter read fails this test.
- **AC5 — seam inertness.** Four negative tests, one per seam, in the exact style of
  `client_event_queue_max_override_requires_test_mode` (`src/daemon_event_subscriptions.rs:914`): each
  asserts `Some(value)` for `("test", raw)` and `None` for `("production", raw)` and `(None, raw)`.
- **AC6 — O(1) diagnostic cost, red first, work-bound not wall-clock.** A test asserts that recording
  `N = 1` and `N = 10_000` observations performs the same bounded work per observation: the histogram
  bucket-selection step count stays exactly one per observation for every input magnitude, and the age-cell
  map length stays constant after first registration, so no per-event growth or scan exists. The test must
  be able to fail: Implement must first demonstrate it red against a deliberately scanning bucket search.
- **AC7 — invariants unchanged.** Existing owner-turn and ready-operation tests stay green, and a diff
  review confirms no constant listed in ticket item 8 changed.
- **AC8 — content blindness.** The three architecture tests stay green:
  `src/unix_terminal_adapter.rs:905`, `src/webrtc_terminal_adapter.rs:915`,
  `src/daemon_attach_stream.rs:1133`.
- **AC9 — gates.**
  1. `cargo fmt --all -- --check`
  2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
  3. Prebuild `botster-session-worker` with locked commands, then `./test.sh --locked` with one test result
     tally and zero failures, per `[[Hub suite runs prebuild the session worker before the locked test wrapper]]`.
  4. `script/run-lifecycle-suite` returns `verdict=clean`.

**Downstream proof.** The Hub charter requires downstream proof when a Hub fix closes a consumer failure.
This ticket adds a surface rather than closing a consumer failure, and the ticket forbids implementing the
saturation campaign here. The downstream obligation is therefore discharged by AC1: every signal the
consumer ticket enumerates is readable through a public daemon request. The consumer run proves the
campaign.

**Worktree hygiene.** `.gitignore` is tracked and non-empty (5 lines) at base. The worktree path
`/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787267568_492780`
contains no `:`, so no `CARGO_TARGET_DIR` override is needed.

## 11. Botster layers touched

Rust hub control plane (daemon transport, owner loop, maintenance), the Hub package event plane, the Hub
Lua runtime invocation boundary, and the in-repository Hub client DTO crate. No TUI, SPA, Rails relay, MCP,
or Workspaces layer is touched. Test harness: Rust unit and integration tests plus the repository lifecycle
suite. No browser or plugin fixture harness is required.

## 12. Vault gaps worth capturing

1. **After A3 resolves** — whether Core's Background deadline waiter reports `TimedOut` while the plugin
   runtime thread is still executing. That fact decides where any future Hub hold seam can live.
2. **After A4 resolves** — whether additive `DaemonStatus` fields are safe without a conformance fixture
   revision bump. The current notes cover revision cutovers but not the additive case.
3. **If AC4 lands as designed** — a concrete note recording the two-surface pattern: keep `try_lock`
   snapshots for ordinary inspection and independent atomics for saturation-time reads. This makes
   `[[saturation counters do not acquire the contended lock they report]]` executable rather than advisory.
4. **The observation-versus-behavior split** used for T1: reading a discriminant that was previously
   discarded closes a correctness gap without changing retirement. That pattern is likely to recur.
