# Plan — Hub: publish bounded event-plane observability counters and four load-campaign seams

- Ticket: `ticket_1787267568_492780`
- Run: `run_1787278338_832165`
- Revision: **8**. Revision 8 answers the three findings in `review_1787288993_904087`: package admission
  rollback omitted the new diagnostic state, a retained consumer queue could hold a stale `Arc`, and stale
  revision 6 instructions still contradicted the revision 7 rules.
- Revision: **7**. Revision 7 answers the four findings in `review_1787288480_333564`: the age list was
  removed while admitted holders were still live, the registry lock could block ingress, the consumer age
  cell still allocated on the event path, and a silent fallback allowed `Accepted` with no age.
- Revision: **6**. Revision 6 answers the three findings in `review_1787287893_907824`: the tombstone
  ring lost a live entry after middle retirement, its storage allocated inside the event path, and AC6
  could not detect either. Section 5 S1a is redesigned and AC6 gains a deterministic allocation control.
- Revision: **5**. Revision 5 adds the sibling ordering protocol in section 14 under human answer
  `question_1787287315_855051`. No technical content changed between revision 4 and revision 5.
- Revision: **4**. Revision 1 drew four findings in `review_1787279337_548281`; revision 2 fixed three and parked on the fourth; revision 3 released the park. Revision 4 answers the three findings in `review_1787286846_900081`.
- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Base: `origin/main` at `b3b54f1` ("Merge ticket: Roll Core pin after IncrementalAttach local-runtime gate")
- Core pin (verified in `Cargo.toml:24-26,43-44`): `7eafa470a18025895995bbedc20d34b58106a03b`

## 0. Response to Plan Review `review_1787288993_904087` (revision 8)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787288993_312281` — package admission rollback omits the new diagnostic state | blocker | **Accepted and fixed.** Verified: `commit_package_generation_locked` (`src/package_event_router.rs:1041-1066`) snapshots via `snapshot_admission` and restores on a failed later subscription, but `AdmissionSnapshot` (`:946-967`) holds only `contracts`, `subscriptions`, `subscriptions_per_plugin`, and `package_generation` — no consumer queues, age lists, or registry entries. A valid-then-invalid batch would roll back contracts while leaving diagnostic state behind. New section S1f takes the preferred option: **no diagnostic state is created until the whole admission succeeds**, so rollback needs no extension and `AdmissionSnapshot` keeps its exact shape. AC17 is the red-first control. |
| `finding_1787288993_721291` — a retained consumer queue can keep an `Arc` the registry no longer exposes | high | **Accepted and fixed.** `inner.consumers` entries are never removed and this plan does not change that, so a retained queue would hold the old `Arc` after the registry dropped its own, and a later subscription could update a stale cell while status read a new one. New section S1g makes the handle `Option<Arc<AgeCell>>`, cleared in the same step that removes the registry entry and rebound at the next subscription admission. A `None` handle on the event path is a counted invariant breach that skips that one consumer, matching the existing per-consumer `continue` at `:493-497`. AC18 is the red-first control. |
| `finding_1787288993_766590` — revision 6 instructions still contradict the revision 7 lifetime and failure policy | high | **Accepted and fixed. This one was my sloppiness.** Revisions 7 appended S1c through S1e but left the original S1a text telling Implement to skip the age update on `None`, remove the list in `apply_unload` with contracts, and skip when the list is absent — active instructions, not marked as superseded. An Implement agent could have followed either path. S1a's capacity and allocation-lifecycle text is rewritten to state the revision 7 and 8 rules and to point at S1c, S1e, and S1f as authoritative. I searched the plan for every remaining skip-on-missing and remove-at-unload instruction; none remain outside the historical review-response sections. |

## 0. Response to Plan Review `review_1787288480_333564` (revision 7)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787288480_816963` — unload removes producer age storage before admitted holders retire | blocker | **Accepted and fixed.** Verified: `apply_unload` (`src/package_event_router.rs:1232-1281`) removes contracts, subscriptions, client holders, and queued copies, but leaves `inner.envelopes`, `inner.admitted`, and `inner.producer` occupancy live, exactly as `[[admitted event holders survive producer unload until Core completion]]` requires. Removing the age list there would strand `producer_slot` on live envelopes, and package replacement would let an old-generation late completion unlink a slot owned by the new generation. Section 5 S1c now keeps **one owner age list** while contracts exist **or** `producer.events > 0`, reuses it across replacement, and removes it only after the last contract is gone and the final admitted holder retires. AC14 is the red-first control. |
| `finding_1787288480_692236` — diagnostic `RwLock` acquisition can block event ingress | blocker | **Accepted and fixed.** Revision 6 had event writers take the registry `RwLock` for each update, which lets a status read, an admission, or an unload delay an accepted event while it holds the router lock. That breaks the project no-wait ingress invariant and changes the router's load class, and AC4 did not cover it. Section 5 S1d now shares each `AgeCell` by `Arc`: the router entry, consumer queue, and mailbox each hold a **direct** `Arc<AgeCell>` and update the atomic through it, while the snapshot registry holds a second `Arc`. **No event path acquires the registry lock.** AC15 is the contention control. |
| `finding_1787288480_967458` — consumer age registration still allocates on the event path | high | **Accepted and fixed.** I fixed this class for producers in revision 6 and missed the identical case for consumers: `try_ingress` inserts the consumer queue at `src/package_event_router.rs:490-492`, so a first-queued-copy cell insertion is new diagnostic allocation during an event. The consumer `AgeCell` is now created at **subscription admission**, retained while a subscription or a queued copy exists, and removed only when both are absent. AC6 part 1 now covers the first queued copy and the consumer shed path. |
| `finding_1787288480_127520` — the plan silently accepts events without producer-age observation | high | **Accepted and fixed.** The revision 6 "skip the age update" fallback converted an invariant breach into missing observability under exactly the load the campaign creates. Section 5 S1e removes it: the slot is reserved **before** any envelope or occupancy mutation, a reservation failure returns a typed non-accepted result rather than `Accepted`, and the failure is counted rather than silent. AC16 asserts every accepted envelope holds exactly one live producer slot through retirement, unload, and replacement. |

## 0. Response to Plan Review `review_1787287893_907824` (revision 6)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787287893_905201` — the tombstone ring overwrites a live oldest entry after out-of-order retirement | blocker | **Accepted. The design was wrong and I have replaced it.** The reviewer's counterexample is exact: `producer.events` counts live entries while `len` counted the occupied span including tombstones, so after a middle retirement the two diverge, the existing shed check admits another event, and `slot = (head + len) % cap` selects `head` and overwrites the live oldest. My claim that the ring "cannot overflow" was false. Section 5 S1a now uses an intrusive doubly-linked age list over preallocated slots plus a free-slot list, which supports exact O(1) arbitrary removal and hole reuse. AC13 is the required full-capacity, middle-retirement, immediate-reacceptance test. |
| `finding_1787287893_967012` — producer age storage allocates during the first event path | blocker | **Accepted and fixed.** Verified: `try_ingress` creates `ProducerOccupancy` through `entry(...).or_insert(...)` at `src/package_event_router.rs:473-478`, which runs **before** the shed check at `:481-483`, so a `Box` created there would allocate inside the event path and even a shed event would allocate it. The age list now lives in its own `RouterInner` map, allocated at **contract admission** (`try_register_contracts`, `try_commit_package_generation`, and `PackageEventRouter::new` for the built-in Hub contracts) and retired in `apply_unload`. `try_ingress` performs a lookup only and never inserts. |
| `finding_1787287893_905133` — AC6 does not prove zero diagnostic allocator calls | high | **Accepted and fixed.** An unchanged pointer and capacity prove only that one buffer did not reallocate, and a self-counted primitive total proves only what the implementation chose to count. AC6 now adds a deterministic allocation control: a `cfg(test)` counting global allocator with a thread-local scope enabled only around the isolated diagnostic update, asserting zero diagnostic allocations for the first accepted event and for a shed event after owner admission. Pre-existing payload and routing allocations stay outside that scope. |

## 0a. Response to Plan Review `review_1787286846_900081` (revision 4)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787286846_451794` — sibling already owns conformance 45 and package 0.1.40 | blocker | **Accepted and fixed.** Verified: `ticket_1787278643_145174` is in Implement (`run_1787282470_625000`, step `run_step_1787284582_430818`) on an approved plan that changes the same DTO, generated protocol, npm mirror, and support metadata. Dependency `dependency_1787286958_412779` is registered. Section 5 S6 point 5 now allocates **revision 46 and package 0.1.41 after that sibling merges**, subject to fresh registry and source checks. |
| `finding_1787286846_827944` — `BTreeSet` violates the no-allocation event-path contract | blocker | **Accepted and fixed.** The reviewer is right: B-tree insertion calls the allocator and does variable comparison work on the accepted-event path, and revision 3 contradicted itself by claiming no per-event allocation. Section 5 S1a replaces it with a preallocated fixed-capacity tombstone ring whose accepted-event update is strictly constant with zero allocator calls. AC6 now counts real hot-path operations. |
| `finding_1787286846_430720` — Web downstream proof invokes Cargo in a Node repository | high | **Accepted and fixed.** Verified: `botster-web` has no `Cargo.toml`; its `package.json` defines `test` as `check-daemon-protocol-drift.mjs` then `App.test.mjs`, plus `typecheck` and `build`. AC10 proof 4 now splits: scratch Cargo patch for `botster-tui` only, and repository-owned npm commands with `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` for `botster-web`. |

## 0b. Response to Plan Review `review_1787279337_548281` (revisions 2 and 3)

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
- `[[admitted event holders survive producer unload until Core completion]]`
- `[[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]`
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
  map growth and no allocator call per event.
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

**S1a. Oldest-age sources (redesigned in revision 6 for `finding_1787287893_905201` and
`finding_1787287893_967012`).**

Consumer queues (`ConsumerQueue.copies: VecDeque`, `src/package_event_router.rs:202-207`) and client
mailboxes (`MailboxInner.events: VecDeque`, `src/daemon_event_subscriptions.rs:120-123`) already expose a
head, so their oldest age is an `O(1)` front read with no new structure.

Producers have no ordered structure: `ProducerOccupancy` (`:197-200`) holds only `events` and `bytes`, and
`retire_holder_locked` (`:1338-1352`) removes envelopes from an unordered `HashMap` in arbitrary order.

Two earlier attempts failed review. Revision 2 used a `BTreeSet`, which allocates and compares on the
event path. Revision 4 used a tombstone ring, which **loses a live entry**: `producer.events` counts live
entries while the ring's `len` counted the occupied span including tombstones, so after a middle
retirement the shed check admits another event and `slot = (head + len) % cap` overwrites the live oldest.

Revision 6 uses an **intrusive doubly-linked age list over preallocated slots, with a free-slot list**:

```rust
const NIL: u32 = u32::MAX;

struct ProducerAgeSlot {
    nanos: u64,   // enqueue time, nanoseconds since the counters base Instant
    prev: u32,    // NIL at the head
    next: u32,    // NIL at the tail; also links the free list
}

struct ProducerAgeList {
    slots: Box<[ProducerAgeSlot]>, // length = policy.producer_queue_max_events (default 256)
    head: u32,   // oldest live entry, NIL when empty
    tail: u32,   // newest live entry, NIL when empty
    free: u32,   // free-list head, singly linked through `next`
}
```

`Envelope` gains `producer_slot: u32`, so retirement addresses its own slot directly.

| Operation | Steps | Cost | Allocator calls |
| --- | --- | --- | --- |
| push (accepted event) | pop `free`, write `nanos`, link at `tail` | strict `O(1)` | zero |
| remove (retirement, any position) | unlink through `prev`/`next`, repair `head`/`tail`, push slot to `free` | strict `O(1)` | zero |
| oldest age (read) | `slots[head].nanos`, or the empty sentinel when `head == NIL` | strict `O(1)` | zero |

There is no scan, no comparison, no amortization argument, and no head-advance loop. Middle retirement is
exact, because unlinking repairs both neighbours instead of relying on tombstone skipping.

**Capacity invariant.** The push happens beside `producer.events += 1` (`:538-539`), which is **after** the
shed check at `:481-483`. The live slot count therefore equals `producer.events` at all times, and both
are bounded by `producer_queue_max_events`. The free list is consequently never empty at push time.
`push` returns `Option<u32>`, and **a `None` result never degrades silently**: S1e requires the caller to
return a typed non-accepted result before any mutation and to count the failure. AC13 asserts `None` never
occurs, and AC16 asserts the fail-closed path when it is forced.

**Allocation lifecycle — moved entirely out of the event path.** Revision 4 put the buffer on
`ProducerOccupancy`, which `try_ingress` creates lazily through `entry(...).or_insert(...)` at
`:473-478`, **before** the shed check. That is inside the event path, and a shed event would allocate too.
The lists therefore live in their own `RouterInner` map. **S1c and S1f are the authoritative lifetime and
admission rules; the summary here must not be read as permitting removal at unload or a silent skip.**

- **Allocated after a whole admission succeeds** (S1f), never on an event path.
- **Retained while `contracts_exist(owner) || producer.events > 0`** and removed only when both are false
  (S1c). Unload alone never removes it.
- **`try_ingress` performs a lookup only.** It calls `get_mut` and never inserts. A missing list is
  structurally unreachable for an accepted event, because `try_ingress` returns `RejectedUndeclared` at
  `:427-436` before producer occupancy. If it ever occurs, S1e fails closed with a typed non-accepted
  result and a counted failure rather than skipping the age update.

**Allocation, stated precisely and completely.** One `Box<[ProducerAgeSlot]>` per admitted producer, at
the S1f admission commit point. At the default bound that is 256 × 16 bytes = 4 KB per producer. **No allocator call
occurs on any accepted-event path or any shed path.** AC6 proves this with a deterministic allocation
control rather than a pointer comparison.

Each published age is stored in an `AgeCell`: one `AtomicU64` holding nanoseconds since a `base: Instant`
captured at counter construction, with `u64::MAX` as the empty sentinel. Writers update the cell while
they already hold the router or mailbox lock, whenever the list head changes. Readers touch only the
atomic.

**S1c. Age-list lifetime across unload and package replacement (fixes `finding_1787288480_816963`).**

`apply_unload` (`src/package_event_router.rs:1232-1281`) removes contracts, subscriptions, client holders,
and queued copies. It deliberately leaves `inner.envelopes`, `inner.admitted`, and `inner.producer`
occupancy intact, because `[[admitted event holders survive producer unload until Core completion]]`
requires admitted Background jobs to keep occupancy until Core completes them. `retire_holder_locked`
(`:1338-1352`) then decrements `producer.events` on that late completion.

Revision 6 removed the age list in `apply_unload`, which is wrong twice over. A late completion would
address a list that no longer exists, and package replacement — unload followed by admitting the next
generation — would let an old-generation completion unlink a slot now owned by the new generation.

Revision 7 ties the list to the same lifetime as producer occupancy:

- **One list per owner.** It is created at first contract admission and **reused across package
  replacement**. Replacement never recreates or swaps the list, so `producer_slot` values on live
  envelopes stay valid across the generation boundary.
- **Retained while `contracts_exist(owner) || producer.events > 0`.** Unload alone never removes it.
- **Removed only when both are false**, checked at exactly two sites: `apply_unload`, when
  `producer.events` is already zero, and `retire_holder_locked`, when the final admitted holder retires
  and no contract remains.

**S1d. Age cells are `Arc`-shared, and no event path takes the registry lock (fixes
`finding_1787288480_692236`).**

Revision 6 had event writers acquire the snapshot registry's `RwLock` on every update. Ingress already
spends its one permitted `try_lock` on `RouterInner`; a second blocking lock would let a status read, a
contract admission, or an unload delay an accepted event while it holds the router lock. That breaks the
project's no-wait ingress invariant and changes the router's load class, which is the exact failure
`[[load diagnostics must not cost work proportional to what they measure]]` warns about.

Revision 7 removes the registry from every event path:

- Each `AgeCell` is created **outside** any event path and shared as `Arc<AgeCell>`.
- The producer age list, the consumer queue, and the client mailbox each hold a **direct**
  `Arc<AgeCell>` and update its `AtomicU64` through that handle.
- The snapshot registry holds a **second** `Arc` to the same cell, purely for enumeration.
- The registry lock is therefore taken only at admission (write) and at status read (read).
  **No accepted-event, shed, or retirement path acquires it.**

**S1e. No silent acceptance without age observation (fixes `finding_1787288480_127520`).**

The ticket requires every producer queue age to be readable. Revision 6's "skip the age update when the
list is absent or `push` returns `None`" fallback would have returned `Accepted` with no age, converting
an invariant breach into missing observability under exactly the saturation the campaign creates.

Revision 7 makes the state structurally unreachable and fails closed if it ever occurs:

- The list exists for every owner that can reach an accepted emit, because `try_ingress` returns
  `RejectedUndeclared` (`:427-436`) before producer occupancy when no contract exists, and S1c keeps the
  list alive while occupancy is nonzero.
- Capacity is guaranteed by the existing shed check (`:481-483`), so the free list is never empty.
- **Order of operations changes:** the slot is reserved **after** the shed check and **before** any
  envelope insert or occupancy increment.
- A reservation failure returns a **typed non-accepted result** (`EventPlaneStatus::ShedFull`) before any
  mutation, never `Accepted`, and increments a dedicated `producer_age_reservation_failures` counter so an
  invariant breach is loud in the same status payload rather than silent.

**S1f. Diagnostic admission is committed only after the whole batch succeeds (fixes
`finding_1787288993_312281`).**

`commit_package_generation_locked` (`src/package_event_router.rs:1041-1066`) takes `snapshot_admission`
before admitting subscriptions sequentially, and calls `restore_admission` if any later subscription
fails. `AdmissionSnapshot` (`:946-967`) carries only `contracts`, `subscriptions`,
`subscriptions_per_plugin`, and `package_generation`. It carries **no** consumer queues, producer age
lists, or snapshot-registry entries.

So a batch whose first subscription is valid and whose later subscription is invalid would roll back
contract state while leaving new age lists, consumer cells, and registry entries behind. That breaks the
documented atomic package-generation commit and the S1b identity bound.

Revision 8 takes the reviewer's preferred option rather than extending rollback: **no diagnostic state is
created until the entire admission is known to succeed.**

- `try_commit_package_generation` already pre-validates through `preview_package_replacement` (`:367`).
  Diagnostic admission moves to a **single commit point after every `subscribe_locked` call returns
  `Accepted`**, so the failure path has nothing to undo and `AdmissionSnapshot` stays untouched.
- `try_register_contracts` (`:300`) applies the same rule: validate the whole contract batch first, then
  create age state once.
- Rollback therefore needs no extension, and `restore_admission` keeps its exact current shape.
- If Implement finds any admission path where a partial failure can still occur after diagnostic
  creation, it must extend rollback to remove **only** state created by that attempt, never disturbing
  pre-existing queues, cells, lists, or live occupancy — and must report that deviation.

AC17 is the red-first control: a mixed valid-then-invalid batch, comparing every admission map **and**
every diagnostic map against the exact pre-call state.

**S1g. A retired consumer age handle is cleared, then rebound (fixes `finding_1787288993_721291`).**

S1d puts a direct `Arc<AgeCell>` on `ConsumerQueue`, and S1b removes the registry entry when neither a
subscription nor a queued copy remains. But `inner.consumers` entries are never removed and this plan
does not change that, so a retained queue would keep the old `Arc` after the registry dropped its own.
A later subscription for the same plugin key could then reuse that queue and update a stale cell while
status reads a newly registered one.

Revision 8 makes the handle explicitly rebindable:

```rust
struct ConsumerQueue {
    events: usize,
    bytes: usize,
    copies: VecDeque<QueuedCopy>,
    age: Option<Arc<AgeCell>>, // None once the identity retires
}
```

- **On retirement** of the consumer identity, the queue's `age` is set to `None` in the same step that
  removes the registry entry, so the old cell can receive no further updates.
- **On the next subscription admission**, the queue's `age` is bound to the **new** registry `Arc`, so the
  event path and the registry reference the same cell.
- **A `None` handle on the event path is an invariant breach, not a silent skip.** It is structurally
  unreachable, because a queued copy requires an admitted subscription, which rebinds first. If it ever
  occurs, Hub counts `consumer_age_binding_failures` and skips **that one consumer**, which matches the
  existing per-consumer behaviour at `:493-497` where an over-capacity consumer is skipped with
  `continue`. This deliberately differs from the producer rule in S1e, where a missing age fails the whole
  emit, because producer age gates acceptance while consumer capacity already has per-consumer semantics.

AC18 is the red-first control for the unsubscribe-or-unload, re-subscribe, first-copy path.

**S1b. Age-cell identity lifetime (fixes the second half of `finding_1787279337_990629`).**
Revision 1 left the three identity maps with no removal rule. Revision 2 slaves each map to the live
identity set that already exists, and removes a cell at exactly the site that ends that identity:

| Map | Key | Insert site | Remove site |
| --- | --- | --- | --- |
| producer ages | package owner | **contract admission**: `try_register_contracts` (`:300`), `try_commit_package_generation` (`:333`), and `PackageEventRouter::new` for the built-in Hub contracts. Never on an event path. Reused across package replacement. | only when `contracts_exist(owner) == false` **and** `producer.events == 0`, checked in `apply_unload` and in `retire_holder_locked` (see S1c) |
| consumer ages | plugin key | **subscription admission** (`try_subscribe`, `try_commit_package_generation`). Never on an event path. | only when the plugin key retains **no subscription and no queued copy**, checked at `apply_unload` and at `unsubscribe` |
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
   - `packages/hub-test-support/package.json` version, `0.1.39` → **`0.1.41`** (see point 5), per
     `[[Hub test support capability cutovers use a new unpublished package version]]`
   - `crates/botster-hub-test-support/src/lib.rs` asset and matrix paths (`:5302`, `:5324`)
   - `docs/client-protocol.md` — explicit client protocol documentation for the new field and revision
   The generated TypeScript must type the new property as **optional** (`observability?: ...`) because the
   Rust field uses `skip_serializing_if`, per `[[generated typescript dtos must encode serde field optionality]]`,
   and the drift check must assert optionality per field, per
   `[[generated dto drift tests need symmetric field and type checks]]`.
5. **Compatibility adjudication — human decision of record, plus sibling collision (revised in revision 4).**

   **Human answer `question_1787286737_531685` settles the ticket-versus-convention conflict.** It reads:
   authorize a conformance revision bump; the `botster-hub-client` convention controls because the ticket
   changes the public `DaemonStatus` shape and generated client artifacts; do **not** hide the new fields
   in an opaque map or alternate representation to preserve revision 44; update the Rust DTO, serialized
   fixture, generated TypeScript, hub-test-support conformance data, documentation, and every revision
   assertion together; and **publish no npm package without separate explicit authorization**. Ticket
   item 5's prohibition is therefore overridden by an explicit human decision, not by planner judgement.

   **Sibling collision, found by Plan Review `finding_1787286846_451794`.** The human answer named
   revision 45 before the collision was known. Sibling Hub `ticket_1787278643_145174` is already in
   Implement (run `run_1787282470_625000`, step `run_step_1787284582_430818`) on an approved plan that
   cuts `CONFORMANCE_FIXTURE_REVISION` 45 and `@trybotster/hub-test-support` 0.1.40 for package notice
   reactions, and it changes the same Hub client DTO, generated daemon protocol, npm mirror, support
   metadata, and client documentation. Two active branches cannot claim the same immutable identity for
   different bytes, per `[[conformance fixture revisions must be unique per published content]]`.

   Resolution, applied here:
   - **Dependency registered:** `dependency_1787286958_412779` makes this ticket depend on
     `ticket_1787278643_145174`. This ticket rebases after that sibling merges.
   - **Allocation moves above the sibling's merged identities:** `CONFORMANCE_FIXTURE_REVISION` **46** and
     `@trybotster/hub-test-support` **0.1.41**. This preserves the human decision's substance — the client
     convention controls and the bump happens — while honouring uniqueness. Implement records revision 46
     as the first fixture containing the event-plane observability fields.
   - **Fresh checks before writing either literal**, per assumption A9. If the sibling's merged numbers
     differ, Implement recomputes rather than trusting these values.
   - **No npm publication.** This ticket cuts the package version in-tree and performs no publish. The
     human answer prohibits publication without separate explicit authorization, and
     `script/publish-npm-packages` is not part of any acceptance check here.
   - `PROTOCOL_VERSION` stays **7**. Framing, request vocabulary, and response semantics are unchanged,
     and an old client deserializes the response unchanged because the field is skipped when empty.
   - `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` stays **36**, per
     `[[additive daemon capabilities do not raise the default client requirement]]`. No new
     operation-specific requirement is introduced, because the field is a status projection rather than a
     capability.

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
- Publishing any npm package. Human answer `question_1787286737_531685` prohibits publication without
  separate explicit authorization. This ticket cuts the in-tree version only.

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
- **Sibling dependency, registered in revision 4.** `dependency_1787286958_412779` makes this ticket
  depend on Hub `ticket_1787278643_145174`, which is already in Implement and changes the same client DTO,
  generated daemon protocol, npm mirror, support metadata, and client documentation. Both tickets target
  the same repository, so this is an ordering edge inside `botster-hub`, not a cross-repository
  prerequisite. This ticket rebases onto that sibling's merge and allocates above its identities.
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
`CONFORMANCE_FIXTURE_REVISION` moves to **46**, `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` stays 36,
npm package `0.1.39` → **`0.1.41`**, and no npm publication occurs. Revision 4 raised the numbers above
sibling `ticket_1787278643_145174`, which already owns 45 and 0.1.40. The remaining execution-time check
is in assumption A9: Implement rechecks the registry and the sibling's merged source before writing either
literal, per `[[conformance fixture revisions must be unique per published content]]`.

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

**A8 — rewritten again in revision 6.** The age list is addressed by slot index carried on
`Envelope.producer_slot`, so envelope-id monotonicity is irrelevant. Two narrower checks remain for
Implement. First, confirm that `try_ingress` is the only path that increments `producer.events`, so the
live slot count cannot drift from it; the capacity invariant that keeps the free list non-empty depends on
that equality. Second, confirm that `retire_holder_locked` is the only path that decrements
`producer.events`, so every push is matched by exactly one removal.

**A9 — new in revision 4.** Conformance revision and package version allocation depends on sibling
`ticket_1787278643_145174` merging first. If that sibling changes its own allocation before merge, or if
`npm view @trybotster/hub-test-support versions` shows anything above `0.1.39` at Implement time, the
numbers in S6 point 5 must be recomputed rather than taken from this plan. Implement performs a fresh
registry and source check before writing either literal.

## 8. Affected surfaces and files

| File | Change |
| --- | --- |
| `src/event_plane_counters.rs` (new) | `EventPlaneCounters`, fixed histogram, `AgeCell`, identity maps, snapshot type |
| `src/lib.rs` | register the new module and re-export the snapshot type |
| `src/package_event_router.rs` | shed by typed reason, admission and delivery attempts, latencies, T2, a new `RouterInner` producer age-list map allocated at contract admission and reused across replacement, `Envelope.producer_slot`, `Arc<AgeCell>` handles on the producer entry and consumer queue, age-list removal gated on no contract and zero occupancy in both `apply_unload` and `retire_holder_locked`, and slot reservation before envelope or occupancy mutation, producer and consumer age cells, unload-time cell removal |
| `src/daemon_event_subscriptions.rs` | overflow gap count, T3 mailbox-expiry count, mailbox age cell, cell removal on connection cleanup |
| `src/daemon_maintenance.rs` | T1 typed completion counting; seam 3 for the two `timeout_ms` sites |
| `src/daemon_transport.rs` | `EgressWriteClass` on `ControlMessage::EgressWriteFailed`, T4 in `record_egress_write_failure`, `enqueued_at` on `ControlMessage::Request` plus its two senders here, the owner-loop serve-site measurement, owner-turn recording, status projection |
| `src/local_webrtc.rs` (**added in revision 2**) | `enqueued_at` at the WebRTC production sender `:1536` and at the test constructions `:4623`, `:6568`, `:6640` |
| `src/runtime.rs` | `hub_test_seams()` and the four gated reads; seam 1 in `take_journal_advanced_wake`; counters accessor |
| `src/lua_runtime.rs` | seam 4 hold before handler invocation |
| `src/client_api.rs` | carry counters from `HubRuntime` to the client-API status body |
| `crates/botster-hub-client/src/lib.rs` | `#[non_exhaustive] DaemonObservabilityCounters`, one new `DaemonStatus` field, `CONFORMANCE_FIXTURE_REVISION` 44 → 46 |
| `crates/botster-hub-client/examples/generate_typescript.rs` | emit the new interface with optional property typing |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | regenerated authoritative artifact |
| `packages/hub-test-support/daemon-protocol.ts`, `index.d.ts`, `package.json` | mirrored artifact and version `0.1.39` → `0.1.41`, cut in-tree with no publish |
| `crates/botster-hub-test-support/src/lib.rs` | support-matrix and asset expectations for the new revision |
| `docs/client-protocol.md` | document the new status field and the revision bump |
| `README.md` | status-surface documentation, if that surface is documented there |
| `docs/plans/...` (this file), `docs/reports/...` (Implement) | plan and report artifacts |

## 9. Risks

- **R1. Observer changes the load class.** Mitigated by fixed arrays, `leading_zeros` bucket selection,
  an intrusive age list over preallocated slots with strict `O(1)` push and removal and zero allocator
  calls on any event path, and the AC6 allocation control plus operation counts.
- **R11 (new in revision 6).** A diagnostic structure can silently lose a live entry when its own
  occupancy accounting diverges from `producer.events`. That is exactly how the revision 4 tombstone ring
  failed review. AC13 is the direct control and must be shown red against that ring.
- **R12 (new in revision 6).** Diagnostic storage can allocate on the first event for an owner if it is
  attached to a lazily created map entry. `try_ingress` creates `ProducerOccupancy` before the shed check
  at `src/package_event_router.rs:473-483`, so the age list is deliberately kept in a separate map that is
  populated at contract admission. AC6 part 1 is the control.
- **R2. A second lock replaces the first.** Revision 6 had event writers take the registry `RwLock`, which
  would have let a status read or an admission delay an accepted event. Revision 7 shares each `AgeCell`
  by `Arc`, so event paths update the atomic through a direct handle and **no event path acquires the
  registry lock**. AC4 covers the status read; AC15 is the ingress-contention control.
- **R13 (new in revision 7).** Diagnostic storage whose lifetime is tied to contracts can be freed while
  admitted holders still reference it, and package replacement can alias a stale slot to a new generation.
  S1c ties the list to `contracts || producer.events > 0` and reuses one list across replacement. AC14 is
  the control.
- **R15 (new in revision 8).** New state added at admission is invisible to an existing rollback snapshot.
  `AdmissionSnapshot` covers four maps only, so any diagnostic map created mid-batch would survive a
  restore. S1f avoids this by committing diagnostic state only after the whole batch succeeds. AC17 is the
  control.
- **R16 (new in revision 8).** A long-lived container that outlives its identity can retain a stale shared
  handle. `inner.consumers` entries are never removed, so the queue's age handle must be cleared at
  retirement and rebound at the next admission. AC18 is the control.
- **R14 (new in revision 7).** A defensive "skip the diagnostic" fallback silently degrades observability
  under exactly the load being measured. S1e replaces it with a fail-closed typed result plus a counted
  failure. AC16 is the control.
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
- **R9 (new in revision 4).** The sibling `ticket_1787278643_145174` could change its own conformance or
  package allocation before merging, which would invalidate 46 and 0.1.41. Assumption A9 requires a fresh
  check at Implement rather than trusting these literals.
- **R8.** `enqueued_at` becomes a required field on an internal enum variant with three production
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
- **AC6 — hot-path work bound and zero diagnostic allocation, red first (rewritten in revision 6).**
  Plan Review noted that an unchanged pointer and capacity prove only that one buffer did not reallocate,
  and that a self-counted primitive total proves only what the implementation chose to count. AC6 now has
  a deterministic allocation control plus operation counts:
  1. **Zero diagnostic allocator calls.** A `#[cfg(test)]` counting global allocator wraps `System` and
     increments a thread-local counter only while an explicit scope guard is active. The guard wraps the
     isolated diagnostic update alone. Assert the count is exactly zero for: the **first accepted event**
     for a newly admitted owner, a **shed event** for an admitted owner, an accepted event at full
     occupancy, a retirement from the middle of the list, the **first queued copy for a consumer**, and a
     **consumer shed path**. Pre-existing payload encoding, `to_string`
     key construction, and routing allocations stay **outside** the guarded scope, per the reviewer's
     instruction, so the assertion measures only diagnostic cost.
  2. **Constant accepted-event operation count.** The recorded operation count for one age-list push is
     identical for `N = 1` and `N = 10_000`, and identical at every occupancy from empty to the policy
     bound. A `BTreeSet`, a scan, or any comparison-based structure fails this.
  3. **Constant retirement cost.** Removal from the head, the tail, and the middle each record the same
     operation count, which is what distinguishes an intrusive list from the rejected tombstone ring.
  4. **Constant histogram cost.** The bucket-selection step count is exactly one per observation at every
     magnitude, including the minimum, the overflow bucket, and every power-of-two boundary.
  Implement must first demonstrate AC6 red against a scanning bucket search, against a comparison-based
  producer age source, and against a variant that allocates the age list inside `try_ingress`.
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
     client pinned to minimum revision 36 accepts a Hub reporting 46, per
     `[[daemon event shape changes bump conformance fixture revision not protocol version]]`.
  3. **Generated-artifact proof.** The generated TypeScript drift check
     (`crates/botster-hub-client/src/lib.rs:4392`) passes, the new property is typed **optional**, and the
     `packages/hub-test-support` mirror plus `package.json` version `0.1.41` match the generated bytes.
     Include an installed-artifact smoke against the locally packed tarball, per
     `[[hub test support npm releases need external consumer smoke]]`. No npm publish occurs.
  4. **Downstream source proof, split by repository language (corrected in revision 4).** Revision 3 ran
     Cargo in both consumers. `botster-web` has no `Cargo.toml`; it is a Node and TypeScript repository.
     - **`botster-tui` (Rust).** Scratch worktree with a temporary `[patch."<git url>"]` redirect to this
       candidate checkout and a separate `CARGO_TARGET_DIR`, running `cargo check --workspace` and
       `cargo check --workspace --all-targets`, per
       `[[scratch cargo patch redirects measure downstream dto breakage]]`. Record the exact failure list.
       Expected: one `cfg(test)` helper at `crates/botster-tui/src/app.rs:26139`.
     - **`botster-web` (Node and TypeScript).** Scratch worktree pointed at the candidate generated file
       through `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL`, which
       `scripts/check-daemon-protocol-drift.mjs:8` accepts as a local override, or through the locally
       packed `@trybotster/hub-test-support` tarball. Then run the repository-owned commands
       `npm test` (which runs `check-daemon-protocol-drift.mjs` and then `src/App.test.mjs`),
       `npm run typecheck`, and `npm run build`, per
       `[[botster web generated protocol drift checks need explicit hub artifact paths]]`.
     - Remove both scratch worktrees afterwards and commit nothing to either consumer.
- **AC11 — diagnostic identity retirement, red first (new in revision 2).** Tests prove each age map
  returns to the live-identity bound after: package unload, client unsubscribe, connection cleanup, and a
  reconnect-churn loop that creates and destroys many connection ids. The churn case must fail when a
  removal site is omitted, which Implement demonstrates by deleting one removal and recording the red run.
- **AC17 — admission rollback leaves no diagnostic residue, red first (new in revision 8).** Submit a
  package generation whose first subscription is valid and whose later subscription is invalid. Capture
  every admission map and every diagnostic map before the call. Assert that after the failure, `contracts`,
  `subscriptions`, `subscriptions_per_plugin`, `package_generation`, `consumers`, the producer age-list
  map, and the snapshot registry each equal their exact pre-call state. Implement must show this red
  against a variant that creates diagnostic state before the batch completes.
- **AC18 — consumer age handle is cleared and rebound, red first (new in revision 8).** Subscribe, queue a
  copy, then unsubscribe or unload so the identity retires. Assert the retained queue's `age` is `None` and
  the registry entry is gone. Re-subscribe the same plugin key, queue a first copy, and assert the event
  path and the registry reference the **same new cell**, that the old cell receives no updates, and that
  the status age reflects the new copy. Implement must show this red against a variant that keeps the
  original `Arc` on the retained queue.
- **AC14 — age-list survival across unload and replacement, red first (new in revision 7).** Admit a
  producer contract, emit an event that Core admits as a Background holder, then unload the producer, then
  admit the next generation (package replacement), then complete the **old** holder late. Assert that the
  old holder's `producer_slot` still addresses the same live list, that its retirement decrements the
  correct occupancy, that it does not unlink a slot owned by the new generation, and that the oldest age
  stays readable throughout. Assert the list is removed only after the last contract is gone **and**
  `producer.events` reaches zero. Implement must show this red against the revision 6 behaviour that
  removed the list in `apply_unload`.
- **AC15 — ingress never waits on the diagnostic registry, red first (new in revision 7).** Hold the
  snapshot registry lock on one thread. On another thread run an accepted emit, a shed emit, and a
  retirement. Assert all three complete without waiting for the registry lock and without returning
  `ShedBusy` for that reason. Implement must show this red against the revision 6 design that acquired the
  registry lock on the event path.
- **AC16 — no accepted event without a live producer slot, red first (new in revision 7).** Assert that
  every accepted envelope holds exactly one live producer slot from acceptance through retirement, across
  unload and package replacement, and that the live slot count equals `producer.events` at every step. Force
  a reservation failure through a test seam and assert the result is a typed non-accepted status, that no
  envelope or occupancy mutation occurred, and that `producer_age_reservation_failures` incremented.
  `Accepted` with a missing age must fail this test.
- **AC13 — producer age-list correctness under out-of-order retirement, red first (new in revision 6).**
  This is the direct control for `finding_1787287893_905201`. Fill a producer to exactly
  `producer_queue_max_events`. Retire an entry from the **middle**. Immediately accept another event.
  Assert that the live oldest entry is unchanged and still readable, that no live slot was overwritten,
  that the live slot count equals `producer.events`, and that `push` never returned `None`. Repeat for
  retirement at the head, at the tail, and in reverse order across the whole list. Implement must
  demonstrate this test **red against the revision 4 tombstone ring**, which overwrites the live oldest
  entry in exactly this sequence.
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
6. **Concurrent branches can collide on an immutable published identity.** Two active Hub tickets each
   selected conformance revision 45 and package 0.1.40 for different bytes, and neither registry history
   nor a source grep would have caught it, because both were unmerged. The durable lesson is that
   conformance and package allocation must check active sibling *runs*, not only published history.
   `[[conformance fixture revisions must be unique per published content]]` covers merged branches; this
   is the in-flight case.
7. **A downstream consumer's proof commands follow its language, not the provider's.** Revision 3 planned
   `cargo check` against `botster-web`, which is a Node and TypeScript repository. A provider-side DTO
   plan must resolve each consumer's own test commands before naming them.
8. **A bounded diagnostic structure needs its own occupancy invariant tied to the value it mirrors.** The
   rejected tombstone ring counted an occupied span while the admission check counted live entries, so a
   middle retirement let a later accepted event overwrite the live oldest value. A fixed-capacity claim is
   not a safety proof unless the structure's own count is the one the capacity check reads.
9. **Diagnostic storage attached to a lazily created map entry allocates on the first event.** In this
   router `try_ingress` creates `ProducerOccupancy` before the shed check, so even a shed event would have
   allocated the buffer. Diagnostic buffers belong on an admission-time lifecycle, not an event-time one.
10. **Diagnostic storage tied to contract lifetime outlives its own removal condition.** Admitted holders
    survive producer unload until Core completion, so anything a live envelope references must survive with
    occupancy, not with contracts. Package replacement makes this sharper, because a recreated structure
    can alias a stale index to a new generation.
11. **A diagnostic registry lock is an ingress lock if any event path touches it.** Sharing the cell by
    `Arc` and keeping the registry for enumeration only is what preserves no-wait ingress.
12. **A defensive skip is an observability outage under load.** Diagnostics for a saturation campaign must
    fail closed with a typed result and a counted failure, never degrade quietly.
13. **New admission-time state must be added to the existing rollback snapshot, or created only after the
    batch commits.** `AdmissionSnapshot` covers four maps, so anything else created mid-batch survives a
    restore silently.
14. **A container that outlives its identity retains stale shared handles.** Because `inner.consumers`
    entries are never removed, a shared age handle must be cleared at retirement and rebound at the next
    admission, or the event path and the status read diverge onto different cells.
15. **Appending a corrected section does not retract the original.** Revisions 7 left superseded S1a
    instructions active beside their replacements, which is an implementation hazard rather than a
    documentation nit. A revision that changes a rule must rewrite the rule, not only add the new one.
16. **Correcting a stale in-repository assumption** — an in-repository workspace member can still be an
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

## 14. Sibling ordering protocol — MANDATORY, do not skip

Human answer `question_1787287315_855051` (`sess-1787091778-0002-0d4e81ae0940d38a6b69bca8c9cfe444`)
directs a **review-only dependency removal**. The engine proved that `override_unmet_gates` does not
bypass a `ticket_dependencies` block, so an override was rejected as the mechanism.

Recorded verbatim in substance: temporarily remove `dependency_1787286958_412779` only to route this run
into Plan Review; keep the sibling rebase, registry, source-check, and identity-allocation requirements in
the plan; if Plan Review requires changes, revise and re-review before starting Implement; after Plan
Review approves, re-add `ticket_1787278643_145174` as a registered dependency **before any Implement
advance**; the observability run then remains parked until the sibling merges; and **do not rely on plan
text alone for implementation ordering**.

### Required sequence

1. **Now.** Remove `dependency_1787286958_412779`. Reason: routing to Plan Review only.
2. **Now.** Request advancement to Plan Review with revision 5.
3. **If Plan Review requires changes.** Revise and re-review. Do not start Implement.
4. **After Plan Review approves, and before any Implement advance.** Re-register the dependency:
   `project_pipelines_add_ticket_dependency(ticket_id="ticket_1787267568_492780", depends_on_ticket_id="ticket_1787278643_145174")`.
   **This step is not optional and is not satisfied by this document.** The human answer states explicitly
   that plan text alone must not carry implementation ordering.
5. **Then park** until `ticket_1787278643_145174` closes. Rebase onto its merge.
6. **Then, at Implement**, run the assumption A9 checks before writing `CONFORMANCE_FIXTURE_REVISION` 46 or
   `@trybotster/hub-test-support` 0.1.41: recheck npm registry history and the sibling's merged source, and
   recompute both literals if the sibling's allocation differs from 45 and 0.1.40.

### Why the edge is removed rather than overridden

The dependency expresses a rebase and identity-allocation constraint that binds **Implement**, not Plan
Review. Reviewing the plan while the sibling is still in Implement costs nothing and surfaces any residual
product defect earlier. The edge is removed for exactly one transition and then restored; it is not
weakened, retired, or replaced by prose.
