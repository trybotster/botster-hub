# Plan — Hub: publish bounded event-plane observability counters and four load-campaign seams

- Ticket: `ticket_1787267568_492780`
- Run: `run_1787278338_832165`
- Revision: **3**. Revision 1 drew four findings in `review_1787279337_548281`. Revision 2 fixed three and parked on the fourth. Revision 3 releases the park after the parent plan approval; see section 13.
- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` at `b3b54f1` ("Merge ticket: Roll Core pin after IncrementalAttach local-runtime gate")
- Core pin (verified in `Cargo.toml:24-26,43-44`): `7eafa470a18025895995bbedc20d34b58106a03b`

## 0. Response to Plan Review `review_1787279337_548281`

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787279337_500928` — human sequencing forbids starting | blocker | **Accepted. This ticket is parked.** See section 13. I will not request a step advance until the parent plan for `ticket_1786663585_879846` is approved. |
| `finding_1787279337_875914` — Hub client DTO ownership and downstream proof missing | blocker | **Accepted and fixed.** `[[botster-hub-client-playbook]]` and its DTO compatibility notes are now loaded. Section 5 S6 decides the Rust source-evolution strategy, names every generated and copied artifact, and settles the conformance revision from actual content. Section 10 adds AC10 downstream proof. |
| `finding_1787279337_990629` — producer oldest-age assumes a queue head that does not exist | high | **Accepted and fixed.** Confirmed: `ProducerOccupancy` (`src/package_event_router.rs:197-200`) holds only `events` and `bytes`, and `retire_holder_locked` (`:1338-1352`) removes from an unordered `HashMap`. Section 5 S1 replaces the design with a bounded ordered id set, and adds explicit identity-retirement rules plus AC11 churn tests. |
| `finding_1787279337_273617` — ready-operation measurement omits the WebRTC sender | high | **Accepted and fixed.** Confirmed a third production sender at `src/local_webrtc.rs:1536`. Section 5 S5 now covers all three production senders and every test construction; section 8 adds `src/local_webrtc.rs`. |

Process note from the review: the Plan `step.completed` event stored empty structured evidence even though
the gate evidence, artifact, and summary were complete. This revision resubmits full gate evidence and
also passes the same fields on the advance call when the park lifts.

## 1. Repository routing

I resolved the run `target_id` through `list_spawn_targets`. `tgt_7e208a0c76a44980a83b63af976b1f22` is
`botster-hub` at `/Users/jasonconigliari/Projects/botster-hub`, repo `trybotster/botster-hub`. I did not
infer the repository from the process working directory.

Repository playbook loaded: `[[botster-hub-playbook]]`.

Second charter loaded: `[[botster-hub-client-playbook]]`. The change adds a field to the public
`DaemonStatus` DTO, and the Hub charter assigns external client DTO ownership to that playbook.

## 2. Playbooks and atomic notes loaded

Role playbooks, in order:

1. `[[planner-playbook]]`
2. `[[botster-planner-playbook]]`
3. `[[botster-hub-playbook]]` (repository ownership charter)
4. `[[botster-hub-client-playbook]]` (public client DTO charter, added in revision 2)

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

Client DTO notes added in revision 2:

- `[[public dto field additions are source breaking without non exhaustive]]`
- `[[scratch cargo patch redirects measure downstream dto breakage]]`
- `[[daemon event shape changes bump conformance fixture revision not protocol version]]`
- `[[generated typescript dtos must encode serde field optionality]]`
- `[[generated dto drift tests need symmetric field and type checks]]`
- `[[additive daemon capabilities do not raise the default client requirement]]`
- `[[Hub test support capability cutovers use a new unpublished package version]]`
- `[[hub test support npm releases need external consumer smoke]]`
- `[[botster web generated protocol drift checks need explicit hub artifact paths]]`
- `[[conformance fixture revisions must be unique per published content]]`

`[[project-pipelines-playbook]]` is **not** loaded. No Project Pipelines package or plugin path is in scope.

## 3. Runtime-teardown class

`teardown_class_applies: false`.

The change adds counters, an internal `ControlMessage` field, one public status field, and four test-mode
configuration reads. It does not change WebRTC or peer lifecycle, `SessionIo`/`ClientWorker` teardown,
multi-peer ownership, resource-spin behavior, or terminal-state versus live-runtime divergence. Scope
item 8 forbids any scheduling or budget change, so no teardown decision moves. I therefore did not load
`[[botster runtime teardown lenses]]`, per the explicit instruction not to apply it outside its class.

## 4. Context loaded (code read at base `b3b54f1`)

- `src/package_event_router.rs`: `EventPlaneStatus` (`:29-42`), `EventPlaneSnapshot` (`:163-172`),
  `ProducerOccupancy` (`:197-200`), `ConsumerQueue` (`:202-207`), `RouterInner` and `PackageEventRouter`
  (`:213-236`), the ingress producer accounting (`:469-539`), the pull and expiry loop (`:569-660`),
  `snapshot()` (`:800-834`), `apply_unload` (`:1236-1256`), and `retire_holder_locked` (`:1317-1353`).
- `src/daemon_event_subscriptions.rs`: `QueuedClientEvent` and `ClientGapSlot` (`:110-142`),
  mailbox overflow gap (`:183-185`), `mark_gap` (`:230-243`), mailbox age expiry (`:264-270`),
  slot and connection removal (`:305`, `:434-437`, `:442`, `:481`),
  `test_client_event_queue_max_from` and its negative test (`:550`, `:913-924`).
- `src/daemon_maintenance.rs`: `MAX_OWNER_TURN_MS = 25` and `MAX_READY_OPERATION_WAIT_MS = 50` (`:34-36`),
  `MaintenanceState.last_owner_turn` (`:619`), the two `timeout_ms: 1_000` admissions (`:1121`, `:1274`),
  and `run_completion_drain_slice` (`:1319-1345`).
- `src/daemon_transport.rs`: owner poll and slice loop (`:339-370`), owner-turn write (`:293`),
  the two `ControlMessage::EgressWriteFailed` senders (`:776`, `:1896`), the write-deadline error
  (`:910-913`), the owner-loop `Request` serve site (`:2552`), `ControlMessage` (`:5296-5345`), and
  `record_egress_write_failure` (`:3072-3079`).
- `src/local_webrtc.rs`: the third production `ControlMessage::Request` sender (`:1536`) and the test
  constructions (`:4623`, `:6568`, `:6640`).
- `src/runtime.rs`: `core_daemon_config` (`:4612-4641`), the `cfg(test)` journal-capacity helper
  (`:4590-4610`), and `take_journal_advanced_wake` (`:3183-3189`).
- `src/lua_runtime.rs`: `DEFAULT_INSTRUCTION_BUDGET = 500_000` (`:55`), the instruction hook (`:553-566`),
  and `LuaPluginRuntime::invoke` (`:591-640`).
- `crates/botster-hub-client/src/lib.rs`: `PROTOCOL_VERSION = 7` (`:30`),
  `CONFORMANCE_FIXTURE_REVISION = 44` (`:31`), `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION = 36` (`:33`),
  `DaemonStatus` (`:2342-2367`), `DaemonLifecycleCounters` (`:2472-2502`, `stalled_writes` at `:2495`),
  the `#[non_exhaustive]` precedent and its stated reason (`:1490-1496`, also `:1548`, `:2994`), and the
  generated-file drift check (`:4392`).
- Generated and copied client artifacts: `crates/botster-hub-client/examples/generate_typescript.rs`,
  `crates/botster-hub-client/generated/daemon-protocol.ts` (`DaemonStatus` at `:808`,
  `lifecycle_counters?` at `:826`), `packages/hub-test-support/daemon-protocol.ts`,
  `packages/hub-test-support/package.json` (version `0.1.39`),
  `crates/botster-hub-test-support/src/lib.rs` (`:5302`, `:5324`, `:5480`, `:6110`).
- Downstream probe (read-only, no consumer checkout modified): the only external Rust `DaemonStatus`
  literal is `botster-tui/crates/botster-tui/src/app.rs:26139`, inside `mod tests` (opened at `:11297`).
  `botster-web` consumes `DaemonStatus` only through its own generated TypeScript at
  `src/botster/generated/daemon-protocol.ts` and `src/botster/connectionDiagnostics.ts`.
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
  comes from `u64::leading_zeros` — one arithmetic step, no loop and no scan, per
  `[[load diagnostics must not cost work proportional to what they measure]]`.
  - **admission latency** is the wall time from entry of the router `try_emit` ingress call to the
    returned `EventPlaneStatus`.
  - **delivery latency** is the wall time from `Envelope.enqueued_at` to the moment a queued copy becomes
    a `ReadyDelivery` in the pull loop.
- Typed timeout counters T1–T3 (see S3).
- Oldest-age cells, redesigned in revision 2 (see S1a and S1b).

**S1a. Oldest-age sources (fixes `finding_1787279337_990629`).**
Revision 1 claimed every queue has a head already in hand. That is true for consumer queues
(`ConsumerQueue.copies: VecDeque`, `src/package_event_router.rs:202-207`) and for client mailboxes
(`MailboxInner.events: VecDeque`, `src/daemon_event_subscriptions.rs:120-123`). It is **false for
producers**: `ProducerOccupancy` (`:197-200`) holds only `events` and `bytes`, and `retire_holder_locked`
(`:1338-1352`) removes envelopes from an unordered `HashMap` in arbitrary order.

The fix adds one ordered field to the existing struct:

```rust
struct ProducerOccupancy {
    events: usize,
    bytes: usize,
    live_envelope_ids: BTreeSet<u64>, // added
}
```

`next_envelope` increases monotonically (`src/package_event_router.rs:227`, `:1348` region), so envelope-id
order is enqueue order. The oldest live producer envelope is therefore
`live_envelope_ids.first()`, and its age is `now - inner.envelopes[&id].enqueued_at`.

- Insert on the accepted-ingress path beside `producer.events += 1` (`:538-539`).
- Remove in `retire_holder_locked` beside the existing producer decrement (`:1348-1351`).
- Both operations are `O(log C)` where `C` is the fixed policy bound `producer_queue_max_events`. `C` does
  not grow with load, so the per-event cost is constant with respect to what is being measured, which is
  what `[[load diagnostics must not cost work proportional to what they measure]]` requires. **The plan
  claims bounded `O(log C)`, not strict `O(1)`, for this one operation.** No lazy pruning loop and no scan
  is introduced.

Consumer and mailbox ages read their existing `VecDeque` front in `O(1)`.

Each published age is stored in an `AgeCell`: one `AtomicU64` holding nanoseconds since a `base: Instant`
captured at counter construction, with `u64::MAX` as the empty sentinel. Writers update the cell while
they already hold the router or mailbox lock. Readers touch only the atomic.

**S1b. Age-cell identity lifetime (fixes the second half of `finding_1787279337_990629`).**
Revision 1 left the three identity maps with no removal rule. Revision 2 slaves each map to the live
identity set that already exists, and removes a cell at exactly the site that ends that identity:

| Map | Key | Insert site | Remove site |
| --- | --- | --- | --- |
| producer ages | package owner | first accepted ingress for that owner | `apply_unload` (`src/package_event_router.rs:1236-1256`), and when `live_envelope_ids` empties and the owner has no contract |
| consumer ages | plugin key | first queued copy for that plugin key | `apply_unload` for that plugin key, and `unsubscribe` when the plugin key retains no subscription |
| mailbox ages | connection id | mailbox creation | `cleanup_client_connection_locked` and the connection removals at `src/daemon_event_subscriptions.rs:434-437` and `:481` |

Note for Implement: `inner.consumers` entries are **not** removed today, so the consumer age map must be
pruned by the rule above rather than by mirroring `inner.consumers`. Implement must not "fix" the router's
own retention as a side effect; that is out of scope.

Bound: each map length must be less than or equal to the count of live identities. AC11 proves the maps
return to the live bound after unload, unsubscribe, connection cleanup, and reconnect churn.

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
  no `PROTOCOL_VERSION` change.

**S4. Oldest queue age as a value (ticket item 2).** Publish the age number for each producer queue, each
consumer queue, and each client mailbox from the S1a age cells. The age predicate at
`src/package_event_router.rs:599` and `src/daemon_event_subscriptions.rs:266` is unchanged.

**S5. Owner turn and ready-operation wait (ticket item 5), corrected in revision 2.**

- `last_owner_turn` is already computed at `src/daemon_transport.rs:293`, in a function that also owns
  `state.lifecycle_counters`. Write `last_owner_turn_us` and a `max_owner_turn_us` high-water value there.
  No change to the private `daemon_maintenance` module boundary is required.
- Ready-operation wait becomes a real production measurement. Add `enqueued_at: Instant` to
  `ControlMessage::Request` and set it at **every** production sender. Revision 1 named two; the complete
  inventory is three:

  | Site | Transport | Notes |
  | --- | --- | --- |
  | `src/daemon_transport.rs:750` | Unix socket connection | `.send(...)` on the connection task |
  | `src/daemon_transport.rs:5236` | socket-path and signal-handler path | `blocking_send` |
  | `src/local_webrtc.rs:1536` | **local WebRTC peer** | `.send(...)`, `grant_id` and `client_id` set |

  Test constructions that must also compile and carry a timestamp:
  `src/local_webrtc.rs:4623`, `:6568`, `:6640`. Owner-loop destructure sites that read the field:
  `src/daemon_transport.rs:2552` (production serve site), and the test destructures at
  `src/daemon_transport.rs:9183`, `:9244`, `:9289`, `:9342`, `:9362`, `:9612` and
  `src/local_webrtc.rs:2596`, `:2668`, `:2795`, `:2989`, `:3009`.

  Every sender stamps `Instant::now()` at the actual send boundary, and the single owner-loop serve site
  at `src/daemon_transport.rs:2552` records `enqueued_at.elapsed()`. Both Unix and WebRTC requests
  therefore reach one common measurement. `ControlMessage` is internal, so no public vocabulary changes.

**S6. Public exposure through the existing status path — revised client DTO decision (fixes
`finding_1787279337_875914`).**

Charter: `[[botster-hub-client-playbook]]` owns this surface. Decisions made **at Plan**, not deferred:

1. **Shape.** `DaemonStatus` gains exactly **one** new field:
   `#[serde(default, skip_serializing_if = "DaemonObservabilityCounters::is_empty")] pub observability: DaemonObservabilityCounters`.
   `DaemonLifecycleCounters` gains **nothing**, so there is no second source break. All new values live on
   the new struct with explicit prefixes: `event_*` for S1 values, `owner_turn_*`, `ready_operation_wait_*`,
   and `stalled_write_timeouts`. `stalled_write_timeouts` carries a doc comment naming it the timeout
   subset of `DaemonLifecycleCounters::stalled_writes`, which stays the unchanged all-failure total.
2. **Rust source-evolution strategy.** `DaemonObservabilityCounters` is `#[non_exhaustive]` and derives
   `Default`, matching the documented precedent and its stated reason at
   `crates/botster-hub-client/src/lib.rs:1490-1496`. Every future counter addition is then free for
   external Rust consumers.
   `DaemonStatus` is **not** marked `#[non_exhaustive]`. Doing so would forbid external struct-expression
   construction entirely and hard-break the existing consumer literal, which is strictly worse than the
   measured one-line cost below. Per
   `[[public dto field additions are source breaking without non exhaustive]]`, this is an accepted,
   measured, coordinated source upgrade rather than an unbounded risk.
3. **Measured downstream cost.** A read-only probe found exactly one external Rust `DaemonStatus` literal:
   `botster-tui/crates/botster-tui/src/app.rs:26139`, inside `mod tests` (opened at `:11297`). Production
   TUI code and all of `botster-web` consume the status through deserialization or generated TypeScript,
   not through a Rust literal. Expected cost: one added field in one `cfg(test)` fixture helper. AC10
   converts this expectation into evidence with a scratch Cargo patch redirect.
4. **Generated and copied artifacts.** All of these change and are named in section 8:
   - `crates/botster-hub-client/examples/generate_typescript.rs` (generator, if it enumerates types)
   - `crates/botster-hub-client/generated/daemon-protocol.ts` (authoritative generated file, drift-checked
     at `crates/botster-hub-client/src/lib.rs:4392`)
   - `packages/hub-test-support/daemon-protocol.ts` and `packages/hub-test-support/index.d.ts` (npm mirror)
   - `packages/hub-test-support/package.json` version, `0.1.39` → `0.1.40`, per
     `[[Hub test support capability cutovers use a new unpublished package version]]`
   - `crates/botster-hub-test-support/src/lib.rs` asset and matrix paths (`:5302`, `:5324`)
   - `docs/client-protocol.md` — explicit client protocol documentation for the new field and revision
   The generated TypeScript must type the new property as **optional** (`observability?: ...`) because the
   Rust field uses `skip_serializing_if`, per `[[generated typescript dtos must encode serde field optionality]]`,
   and the drift check must assert optionality per field, per
   `[[generated dto drift tests need symmetric field and type checks]]`.
5. **Compatibility adjudication, settled from actual content.**
   - `PROTOCOL_VERSION` stays **7**. Framing, request vocabulary, and response semantics are unchanged, and
     an old client deserializes the response unchanged because the field is skipped when empty.
   - `CONFORMANCE_FIXTURE_REVISION` moves **44 → 45**. Per
     `[[daemon event shape changes bump conformance fixture revision not protocol version]]`, an additive
     shape change that alters first-party fixture JSON and downstream deserialization expectations bumps
     the revision. The generated TypeScript, the npm mirror, and the support matrix all change, so
     downstream must notice. `ensure_compatible` treats the revision as a floor, so this is not a flag day.
   - `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` stays **36**, per
     `[[additive daemon capabilities do not raise the default client requirement]]`. No new operation-specific
     requirement is introduced, because the field is a status projection rather than a capability.
   - Revision allocation must be checked against published content before Implement commits `45`, per
     `[[conformance fixture revisions must be unique per published content]]`.

   **Stated conflict with the ticket.** Ticket item 5 says not to add a "conformance fixture revision bump
   if the existing status path can carry them". The existing status path does carry the values, and no new
   transport or request vocabulary is added, which is what that clause protects. The revision bump is a
   separate downstream-drift signal that the repository convention requires whenever generated client
   artifacts change. Plan chooses the convention and states the conflict rather than resolving it silently.
   If Plan Review prefers the literal ticket clause, the alternative is to leave the values out of the
   public DTO entirely, which would fail acceptance line 1.

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
- `PROTOCOL_VERSION` bump, new transport, and new request vocabulary.
- Any Core source change. Core already publishes every typed kind this plan reads.
- Repairing the router's own `inner.consumers` retention. The consumer age map is pruned independently.
- Any change to retirement, gap delivery, shed decisions, or completion semantics. Observation only.
- Committing anything to `botster-tui` or `botster-web`. AC10 uses scratch worktrees and removes them.

## 6. Repository ownership boundaries and cross-repository dependencies

- **Hub owns** T2, T3, T4, the counters module, the status projection wiring, the owner-turn and
  ready-operation-wait measurements, and all four test seams. All are host-profile policy and
  control-plane observation, which `[[botster hub is a first party host profile over core]]` assigns to Hub.
- **Hub Client owns** the public DTO shape, the compatibility descriptor values, the generated TypeScript,
  and the conformance revision, per `[[botster-hub-client-playbook]]` and
  `[[botster hub client crate is the external client boundary]]`. That crate is an in-repository workspace
  member (`Cargo.toml:4,20`) rather than a separate spawn target, so the change lands in this run under
  that charter's rules. **Revision 1 wrongly used in-repository location as a reason to skip the charter.**
- **Core owns** the authoritative T1 signal: `PluginInvocationFailureKind::TimedOut`
  (`crates/botster-core/src/contract/actor.rs:1233`), produced by the deadline waiter at
  `crates/botster-core/src/engine/plugin_worker.rs:2331-2421`. Hub only reads a discriminant it already
  receives. **No Core change is required, so this run registers no Core dependency ticket.** The Core pin
  stays at `7eafa470a18025895995bbedc20d34b58106a03b`.
- **Downstream consumers.** `botster-tui` (`tgt_c3d470bab78549df920a41e8fb0e58d8`) has one `cfg(test)`
  Rust literal to update. `botster-web` (`tgt_40abcf71ccf049f4ac0c99953a799869`) consumes the generated
  TypeScript and has its own generated copy plus a drift check that needs an explicit Hub artifact path,
  per `[[botster web generated protocol drift checks need explicit hub artifact paths]]`. AC10 measures
  both costs from this run without committing to either repository. If AC10 shows a required consumer
  edit beyond a `cfg(test)` helper, Implement must stop and this run must register a dependency ticket
  against that consumer's target rather than editing it here.
- **Data plane untouched.** No terminal bytes, scrollback, or per-client egress payload is inspected, per
  `[[botster data plane bypasses the hub through session and client actors]]`. T4 classifies an
  `io::ErrorKind` only; it never reads the frame body.
- **Consumer dependency edge.** `ticket_1786663585_879846` consumes this surface. The parent Plan Review
  states that this ticket's dependency edge must be restored before that ticket's Implement step.
  Restoring that edge is the parent run's action, so this plan adds and removes no dependency edge.

## 7. Assumptions and unknowns

**A1 — sequencing. Resolved against revision 1.** Plan Review finding `finding_1787279337_500928` ruled
that human answer `question_1787267931_572353` forbids this ticket from starting before the parent
integration plan is approved. Revision 2 accepts that ruling. This ticket is parked; see section 13.

**A2.** Admission latency and delivery latency have no existing definition in this repository. S1 defines
them. If the campaign needs different boundaries, that must be settled at Plan Review, because the
measurement points are hard to move after Implement.

**A3.** I assume Core's deadline waiter returns `TimedOut` for a Background invocation whose runtime
thread is still inside `LuaPluginRuntime::invoke`, so seam 4 can produce T1. Implement must prove this with
the red-first test in AC2 before building anything on top of it. If Core instead waits for the runtime to
return, seam 4 needs a different hold point and Implement must report that finding rather than reshape the
counter.

**A4 — resolved in revision 2, no longer deferred.** The client contract decision is settled in S6:
one new `#[non_exhaustive]` struct, one new `DaemonStatus` field, `PROTOCOL_VERSION` stays 7,
`CONFORMANCE_FIXTURE_REVISION` moves to 45, `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` stays 36, npm
package `0.1.39` → `0.1.40`. The remaining execution-time check is narrow: Implement must confirm that no
published content already claims revision 45 before committing it, per
`[[conformance fixture revisions must be unique per published content]]`.

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

**A8 — new in revision 2.** The producer age uses `BTreeSet<u64>` keyed by envelope id and relies on
`next_envelope` being monotonic per router instance. Implement must confirm that no path reuses or resets
an envelope id within one router lifetime. If ids can repeat, the age source must key on
`(enqueued_nanos, envelope_id)` instead, at the same bounded cost.

## 8. Affected surfaces and files

| File | Change |
| --- | --- |
| `src/event_plane_counters.rs` (new) | `EventPlaneCounters`, fixed histogram, `AgeCell`, identity maps, snapshot type |
| `src/lib.rs` | register the new module and re-export the snapshot type |
| `src/package_event_router.rs` | shed by typed reason, admission and delivery attempts, latencies, T2, `ProducerOccupancy.live_envelope_ids`, producer and consumer age cells, unload-time cell removal |
| `src/daemon_event_subscriptions.rs` | overflow gap count, T3 mailbox-expiry count, mailbox age cell, cell removal on connection cleanup |
| `src/daemon_maintenance.rs` | T1 typed completion counting; seam 3 for the two `timeout_ms` sites |
| `src/daemon_transport.rs` | `EgressWriteClass` on `ControlMessage::EgressWriteFailed`, T4 in `record_egress_write_failure`, `enqueued_at` on `ControlMessage::Request` plus its two senders here, the owner-loop serve-site measurement, owner-turn recording, status projection |
| `src/local_webrtc.rs` (**added in revision 2**) | `enqueued_at` at the WebRTC production sender `:1536` and at the test constructions `:4623`, `:6568`, `:6640` |
| `src/runtime.rs` | `hub_test_seams()` and the four gated reads; seam 1 in `take_journal_advanced_wake`; counters accessor |
| `src/lua_runtime.rs` | seam 4 hold before handler invocation |
| `src/client_api.rs` | carry counters from `HubRuntime` to the client-API status body |
| `crates/botster-hub-client/src/lib.rs` | `#[non_exhaustive] DaemonObservabilityCounters`, one new `DaemonStatus` field, `CONFORMANCE_FIXTURE_REVISION` 44 → 45 |
| `crates/botster-hub-client/examples/generate_typescript.rs` | emit the new interface with optional property typing |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | regenerated authoritative artifact |
| `packages/hub-test-support/daemon-protocol.ts`, `index.d.ts`, `package.json` | mirrored artifact and version `0.1.39` → `0.1.40` |
| `crates/botster-hub-test-support/src/lib.rs` | support-matrix and asset expectations for the new revision |
| `docs/client-protocol.md` | document the new status field and the revision bump |
| `README.md` | status-surface documentation, if that surface is documented there |
| `docs/plans/...` (this file), `docs/reports/...` (Implement) | plan and report artifacts |

## 9. Risks

- **R1. Observer changes the load class.** Mitigated by fixed arrays, `leading_zeros` bucket selection,
  no allocation on any per-event path, a policy-bounded `BTreeSet`, and the AC6 work-bound test.
- **R2. A second lock replaces the first.** The age maps carry their own `RwLock`. Writers hold it for one
  atomic store, and readers never touch the router mutex. AC4 proves the read path returns values while
  the router lock is held.
- **R3. Public DTO break.** Now measured rather than assumed: one external `cfg(test)` literal. AC10
  converts the estimate into evidence before Implement claims compatibility.
- **R4. Accidental behavior change.** T1 must keep `run_completion_drain_slice` retirement identical, and
  T3 must not change gap-bit or `EventGap` semantics. Reviewer instruction: diff these two functions for
  control-flow change, not only for added lines.
- **R5. Seam leakage into production.** Mitigated by AC5's four negative tests and by clamping every value.
- **R6. A6 could be wrong about where the owner turn is observable.** If the write site does not have
  `lifecycle_counters` in scope after the change, Implement must report it before widening module
  visibility.
- **R7 (new).** Diagnostic identity maps could still grow under churn if a removal site is missed. AC11 is
  the direct control, and it must be shown red against a deliberately omitted removal.
- **R8 (new).** `enqueued_at` becomes a required field on an internal enum variant with three production
  senders and six test constructions. A missed site is a compile error rather than a silent default, which
  is deliberate: the field must not have a `Default`.

## 10. Acceptance checks and tests

Every ticket acceptance line maps to a check. AC2, AC3, AC4, AC6, and AC11 are red-first: Implement must
record the failing output **before** the change, per
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
- **AC6 — bounded diagnostic cost, red first, work-bound not wall-clock.** A test asserts that recording
  `N = 1` and `N = 10_000` observations performs the same bounded work per observation: the histogram
  bucket-selection step count stays exactly one per observation for every input magnitude, and the
  producer `live_envelope_ids` length never exceeds the fixed `producer_queue_max_events` policy bound.
  Implement must first demonstrate it red against a deliberately scanning bucket search.
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
- **AC10 — client DTO proof (new in revision 2).** Four separate proofs, per
  `[[botster-hub-client-playbook]]`'s gate to separate serde wire proof from downstream source proof:
  1. **Wire proof.** A serde test shows an old-shaped `DaemonStatus` JSON without the new key still
     deserializes, and that an empty `observability` value is omitted from the serialized frame.
  2. **Protocol-versus-revision proof.** Assert exact `PROTOCOL_VERSION` equality is unaffected, and that a
     client pinned to minimum revision 36 accepts a Hub reporting 45, per
     `[[daemon event shape changes bump conformance fixture revision not protocol version]]`.
  3. **Generated-artifact proof.** The generated TypeScript drift check
     (`crates/botster-hub-client/src/lib.rs:4392`) passes, the new property is typed **optional**, and the
     `packages/hub-test-support` mirror plus `package.json` version `0.1.40` match the generated bytes.
     Include an installed-artifact smoke, per `[[hub test support npm releases need external consumer smoke]]`.
  4. **Downstream source proof.** Scratch Cargo patch redirect against `botster-tui` and `botster-web`
     worktrees with a separate `CARGO_TARGET_DIR`, running `cargo check --workspace` and
     `cargo check --workspace --all-targets`, per
     `[[scratch cargo patch redirects measure downstream dto breakage]]`. Record the exact failure list.
     Expected: one `cfg(test)` helper in `botster-tui`. Remove the scratch worktrees afterwards and commit
     nothing to either consumer.
- **AC11 — diagnostic identity retirement, red first (new in revision 2).** Tests prove each age map
  returns to the live-identity bound after: package unload, client unsubscribe, connection cleanup, and a
  reconnect-churn loop that creates and destroys many connection ids. The churn case must fail when a
  removal site is omitted, which Implement demonstrates by deleting one removal and recording the red run.
- **AC12 — ready-operation wait covers WebRTC (new in revision 2).** A test proves that a request arriving
  through the local WebRTC sender (`src/local_webrtc.rs:1536`) reaches the same owner-loop measurement as a
  Unix request, and that both produce a non-absent ready-operation-wait observation.

**Downstream proof.** The Hub charter requires downstream proof when a Hub fix closes a consumer failure.
This ticket adds a surface rather than closing a consumer failure, and the ticket forbids implementing the
saturation campaign here. AC1 discharges the campaign-facing obligation: every signal the consumer ticket
enumerates is readable through a public daemon request. AC10 discharges the client-crate obligation for
`botster-tui` and `botster-web`.

**Worktree hygiene.** `.gitignore` is tracked and non-empty (5 lines) at base. The worktree path
`/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787267568_492780`
contains no `:`, so no `CARGO_TARGET_DIR` override is needed for this repository. AC10's scratch consumer
worktrees still use their own separate `CARGO_TARGET_DIR`.

## 11. Botster layers touched

Rust hub control plane (daemon transport, owner loop, maintenance), the local WebRTC request path, the Hub
package event plane, the Hub Lua runtime invocation boundary, the in-repository Hub client DTO crate, and
its generated TypeScript and npm mirror. No TUI, SPA, Rails relay, MCP, or Workspaces source is edited in
this run; those repositories are only compiled read-only as AC10 evidence. Test harness: Rust unit and
integration tests, the repository lifecycle suite, and scratch consumer `cargo check` runs.

## 12. Vault gaps worth capturing

1. **After A3 resolves** — whether Core's Background deadline waiter reports `TimedOut` while the plugin
   runtime thread is still executing. That fact decides where any future Hub hold seam can live.
2. **After A8 resolves** — whether router envelope ids are monotonic for a router lifetime, which is what
   makes an id-ordered age source valid.
3. **If AC4 lands as designed** — a note recording the two-surface pattern: keep `try_lock` snapshots for
   ordinary inspection and independent atomics for saturation-time reads. This makes
   `[[saturation counters do not acquire the contended lock they report]]` executable rather than advisory.
4. **If AC11 lands as designed** — a note that observability identity maps need an explicit retirement site
   per identity class, because a counter map keyed by a churning identity is an unbounded-growth path that
   repeated-observation tests cannot detect.
5. **The observation-versus-behavior split** used for T1: reading a discriminant that was previously
   discarded closes a correctness gap without changing retirement. That pattern is likely to recur.
6. **Correcting a stale in-repository assumption** — an in-repository workspace member can still be an
   external contract surface. Revision 1 used crate location to skip a charter, which the Hub charter's
   "does not own" list already forbids.

## 13. Park status — RELEASED

This plan was parked at revision 2 and is **released as of revision 3**.

Plan Review finding `finding_1787279337_500928` ruled that human answer `question_1787267931_572353`
forbids this ticket from starting until Plan Review approves the parent integration plan for
`ticket_1786663585_879846`.

That condition is now satisfied. I verified it independently rather than accepting the notification alone:

- Parent run `run_1787262311_549251`, Plan Review visit at step 14 (started `1787279449`), produced
  `review_1787279657_551348` with verdict **approved** at `1787279657`. It is the newest review on that
  run by timestamp, and it supersedes `review_1787278903_443047`.
- The parent dependency edge on this ticket is restored: `dependency_1787279676_288569`, created at
  `1787279676`, `depends_on_ticket_id = ticket_1787267568_492780`.
- Parent gate result: `gate_result_1787279666_738333`.

**Correction to the revision 2 release condition.** Revision 2 listed edge restoration as a second step
before this run could proceed. The start condition is the parent Plan Review **approval** alone. This run
does not wait for the parent dependency edges to close, and it does not wait for parent Implement. The
parent Implement step is itself parked until this ticket and the other four prerequisites close, so this
ticket is now on the critical path rather than behind it.

Revision 3 therefore requests advancement to Plan Review. Nothing else in this plan changed between
revision 2 and revision 3; the technical content is identical.
