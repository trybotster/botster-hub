# Plan — Hub: publish bounded event-plane observability counters and four load-campaign seams

- Ticket: `ticket_1787267568_492780`
- Run: `run_1787278338_832165`
- Revision: **13**. Revision 13 answers the three findings in `review_1787292035_194663`: repeated
  replacement had no unambiguous gate rule, the public age observation DTO was unspecified, and the loom
  gate was not executable from the planned change set.
- Revision: **12**. Revision 12 answers the four findings in `review_1787291409_401120`: the opening RMW
  needed `AcqRel` not `Release`, `Envelope` needed generation-specific list identity, the publication gate
  had to move onto the cell so the lock-free snapshot could read it, and the affected-file table still
  mandated the withdrawn reset-in-place design. Section 0y records that my revision 11 zero-reference
  sweep claim was false.
- Revision: **11**. Revision 11 answers the five findings in `review_1787290799_177961`. The decided
  bounded-staleness contract stands; revision 10 did not yet satisfy its no-prior-generation and
  explicit-indeterminate rules. Three fixes: a two-phase odd/even version, a fresh cell per generation, and
  a latched invalid state. The artifact for this revision is created **after** the final commit, per the
  process finding.
- Revision: **10**. Human decision `question_1787290055_403092` reclassifies oldest age as a bounded
  diagnostic observation. The S1 design is rewritten and roughly halves in size. Revision 10 also corrects
  a false statement in revision 9 gate evidence; see section 0z.
- Revision: **9**. Revision 9 answers the two findings in `review_1787289501_106968`: producer retirement
  could not both prune the registry and avoid its lock, and S1f attributed `preview_package_replacement`
  to the wrong entry point. The cleanup fix removes a requirement rather than adding one.
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

## 0. Response to Plan Review `review_1787292035_194663` (revision 13)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787292035_541858` — repeated replacement has no current-gate decrement rule | blocker | **Accepted.** "Decrement the successor" is ambiguous once three generations overlap: with N replaced by N+1 then N+2 before N drains, N+1 is already retired and write-closed, so a late N retirement aimed at its immediate successor would leave N+2's gate permanently open and N+2 would never publish an age. S1c now states the rule exactly: `ProducerOccupancy` carries `outstanding_prior`, a replacement seeds the **new current cell's** gate from the total live envelopes across **all** prior generations, and a retirement whose `envelope.producer_age_ref.generation` differs from the current generation decrements `outstanding_prior` and the **current** cell only. Both are `O(1)`. New AC20b runs N → N+1 → N+2 with interleaved late retirements and is red against the revision 12 rule. |
| `finding_1787292036_111963` — the public age observation DTO remains undefined | blocker | **Accepted; this was a public client-contract decision the charter assigns to Plan and I had left to Implement.** New section S6a specifies `DaemonQueueAgeObservation`, `DaemonQueueKind`, and `DaemonQueueAgeState` with the three decided states distinct, identity keys, producer generation, microsecond units, `Option` fields so generated TypeScript emits optional properties, and a `#[serde(other)] Unknown` arm for forward tolerance — because `#[non_exhaustive]` alone does **not** make serde accept an unseen variant. AC1 and AC10 gain exact wire cases for all three states, the missing-cell row, and the unknown-state case. |
| `finding_1787292036_587759` — the loom model check is not wired into repository scope | high | **Accepted.** I named a gate that could not run: `loom` appears in no manifest and no lockfile. AC19 case 0 now specifies a `[target.'cfg(loom)'.dev-dependencies]` entry that leaves the normal build untouched, names the command `RUSTFLAGS="--cfg loom" cargo test --lib queue_age_model`, adds it to AC9 as command 5, and adds `Cargo.toml` and `Cargo.lock` to the affected-file table. **I did not verify the current `loom` version from the registry**, so Implement pins it and records the exact version; if the locked-build policy blocks adding it, Implement must say the ordering claim is unproven rather than dropping case 0 quietly. |

## 0. Response to Plan Review `review_1787291409_401120` (revision 12)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787291409_211318` — the opening version update does not order later field stores | blocker | **Accepted; the ordering was wrong.** `Release` on the opening RMW stops earlier accesses moving after it; it does **not** stop later relaxed stores moving before it, so a reader could observe changed fields while still seeing the old even version. S1b now uses **`AcqRel` for the opening RMW** and `Release` for the closing one, and the reader uses the canonical shape: `Acquire` first load, relaxed field loads, `atomic::fence(Acquire)`, then the second load. Every mutable field including `invalid` and `gate` travels inside the same bracket, so AC21 needs no second protocol. AC19 gains a **`loom` model check** as case 0, because deterministic scheduling tests cannot substantiate an ordering claim. |
| `finding_1787291409_910821` — late retirement has no generation-specific age-list identity | blocker | **Accepted.** Revision 11 asserted old holders "retire against their own list" while `Envelope` carried only a bare `producer_slot: u32` and the router held one age-list map keyed by owner; a slot number alone cannot select the old generation's list after replacement, so AC14 and AC20 demanded behaviour the state shape could not implement. S1c adds `ProducerAgeRef { generation, slot }` on `Envelope`, keys lists by `(owner, generation)`, gives each list a `live` count, and drops a non-current generation's list and retired cell when `live` reaches zero. Every prior list is guaranteed to drain because each of its envelopes retires exactly once. |
| `finding_1787291409_621659` — the snapshot cannot observe `prior_generation_holders` | blocker | **Accepted.** I put the publication gate on `ProducerOccupancy` inside `RouterInner`, which S2 forbids the saturation snapshot from touching, so the gate the reader depends on was unreadable by that reader. The gate moves onto the cell as `QueueAgeMetric.gate`, seeded at replacement and decremented in `retire_holder_locked`, both **inside the two-phase bracket**, so it is covered by the same consistency protocol as `count` and `oldest_nanos`. AC20 now asserts the indeterminate window **through the public snapshot path**. |
| `finding_1787291409_639376` — the affected-file table still mandates reset-in-place | high | **Accepted, and the sweep claim attached to it was false.** See section 0y. The `src/package_event_router.rs` row and R17 are rewritten to the current design, and the `src/event_plane_counters.rs` row, S1e, A8, R13, AC14, and AC20 carried the same staleness and are corrected too. Revision 12 gate evidence quotes the **literal sweep command and its literal output** instead of asserting a count. |

## 0. Response to Plan Review `review_1787290799_177961` (revision 11)

The human decision stands and is not reopened. Revision 10 adopted its *classification* but did not yet
satisfy two of its explicit rules: never publish an age from a retired or reused generation, and represent
an indeterminate sample explicitly. Three of these findings are that gap, and each fix removes a way for
the design to be wrong rather than adding a feature.

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787290799_815700` — a trailing-only mutation stamp misses an in-progress reset | blocker | **Accepted; the protocol was unsound.** A reader could load `v1`, read fields while the writer was mid-store, and load an unchanged `v2`, accepting a mixed sample — and across a generation change that pairs a new generation with an old age. S1b replaces the trailing bump with a **two-phase odd/even version** that marks the update in progress *before* any field changes. AC19 case 3 pauses a writer with the version odd and requires indeterminate; it is red against revision 10. |
| `finding_1787290799_555137` — one producer age list cannot supply a fresh replacement generation | blocker | **Accepted.** Old-generation holders survive unload per `[[admitted event holders survive producer unload until Core completion]]`, so one list holds old and new slots at once, and an old slot at the head would publish a retired generation's age. S1c stops resetting one shared cell: **each generation gets its own cell with an immutable `generation` field**, the previous cell is retired and write-closed on the same control path, and the new generation reports **indeterminate until `prior_generation_holders` reaches zero** — the reviewer's own suggested rule, maintained in `O(1)`. AC20 is the control. |
| `finding_1787290799_691196` — diagnostic failure has no persistent indeterminate state | blocker | **Accepted.** Revision 10 said "the sample becomes indeterminate" with nowhere to record it, so an accepted envelope with no producer slot left the list permanently incomplete while later samples looked stable. S1f adds `invalid: AtomicBool`, **latched** on any diagnostic failure and checked inside the read bracket, cleared only by the arrival of a new generation with a fresh cell. A missing cell appears in the snapshot as an explicit indeterminate entry, never an omitted row. AC21 is the control. |
| `finding_1787290799_270331` — admission cannot prune cells that retained queues still reference | high | **Accepted.** "Retired and unreferenced" is unusable: the registry owns an `Arc`, `inner.consumers` entries are never removed and keep theirs, and an empty live queue is indistinguishable from a retired one. S1d makes membership **explicit lifecycle state** recorded only on control paths, keyed by identity **and generation**, and lets admission remove a retired entry even while a container keeps its direct `Arc` to the now write-closed cell. AC11 gains the retained-`ConsumerQueue` case and an assertion that a live-but-empty queue is not pruned. |
| `finding_1787290799_720688` — the latest artifact does not identify the submitted plan commit | low, process | **Accepted, and it was my own verification pass that caused it.** I created `artifact_1787290486_548537` at `e40e08d`, then found R10 missing, committed `cf58857`, and never re-issued the artifact. Revision 11 creates its artifact **only after the final plan commit**, and the gate evidence names that same commit. |

## 0y. Correction of the false revision 11 sweep claim

Revision 11 gate evidence and artifact stated that a scripted sweep found **zero** references to the
withdrawn design in active text. **That claim was false.** The committed plan still contained
`QueueAgeMetric.mutations`, "cell **reset** ... at identity retirement", and "`generation` bump plus reset
when a later generation is admitted" in the `src/package_event_router.rs` affected-file row, plus a reset
cell justification in R17. Plan Review `finding_1787291409_639376` found them.

Cause: my sweep matched a hand-written list of strings I chose from memory. It contained
`` `mutations` `` only in the forms `mutations: AtomicU64` and ``bump `mutations` last``, so the row's
`` `mutations` bump `` never matched. A sweep keyed on what I remember writing cannot find what I forgot to
change.

**This is the third false verification claim in this run**, after the revision 9 "AC11 tightened" claim and
the revision 10 R10 citation. The first two I found myself; this one a reviewer found. The pattern is
consistent: I assert a verification conclusion instead of showing the check.

Method change for revision 12 and after: I do not assert sweep results. Gate evidence carries the **literal
command and its literal output**, so a reader can see what was matched rather than trusting a summary. Where
a count is claimed, the command that produced it is quoted next to it.

## 0z. Correction of false revision 9 gate evidence

Revision 9 gate evidence (`gate_result_1787289800_918473`) and artifact `artifact_1787289710_974650`
stated that AC11 was tightened. **That statement was false.** AC11's body had not changed since revision 2;
`git log -S` on its text confirms the last change was commit `ddc09dc`. Only its header line changed in
revision 9.

Cause, stated plainly so it is not repeated: my plan edits ran several string replacements in one script
and wrote the file once at the end. When any anchor failed to match, the script aborted and discarded
**every** edit in that script, including ones that had already been computed. I then verified by grepping
for new strings that other, successful scripts had written, so the gaps looked closed. The same mechanism
produced `finding_1787288993_766590` in review five, where superseded S1a instructions stayed active.

Corrective action, per the human decision: each plan correction in revision 10 is applied as a **separate
edit, verified immediately after it is applied**, and the final diff is inspected before the gate is
submitted. Every acceptance item cited in revision 10 gate evidence was re-read in the committed file
rather than assumed.

## 0. Response to Plan Review `review_1787289951_879587` and human decision `question_1787290055_403092` (revision 10)

The decision reclassifies ticket item 2. Oldest age is a **bounded diagnostic observation**, not
authoritative queue state, and needs no exact linearizable snapshot. That single change dissolves two of
the three findings rather than patching them, and the S1 design roughly halves in size.

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787289951_457652` — S1h does not separate age-list removal from registry pruning | blocker | **Dissolved by the decision, not patched.** The finding is correct about revision 9: I conflated the `RouterInner` age list with the snapshot-registry `Arc`, and a later admission could reuse a list whose cell was still marked dead, so a live producer would report no age. Under the decided model there is no dead flag and no lifetime split. Retirement **resets** the cell, a reset cell reports no usable age by contract, and a reused generation bumps `generation` and resets again, so S1c handles reuse in three stores. Registry pruning becomes a **memory bound only**, because a lingering entry is now harmless for correctness. |
| `finding_1787289951_243880` — AC11 still requires immediate producer registry removal | high | **Accepted; this was the stale text from my broken edit process, disclosed in section 0z.** AC11 is rewritten against the decided model, with an exact numeric bound the churn test computes from `N` rather than the hand-wavy "roughly twice the peak live count" I wrote in revision 9. |
| `finding_1787289951_802643` — dead-marker reads have no concurrency ordering rule | high | **Dissolved by the decision.** The finding is correct that revision 9 defined no write order, read order, or memory ordering across two atomics. The decided protocol replaces exactness with a bounded consistency read, so the only ordering needed is one release-ordered `mutations` bump after the value stores and one acquire-ordered load on each side of the sample. There is no second protocol to get wrong. AC19 is the direct control, including the ABA case. |

**One deliberate tightening of the decided protocol, flagged rather than substituted silently.** The
decision names a `count`-age-`count` bracket. A raw count is not monotonic, so a `5 → 6 → 5` sequence
would pass that bracket while the sample is two mutations stale, exceeding the stated one-mutation bound.
S1b therefore brackets on a **monotonic `mutations` stamp** while the sample still carries `count`. Same
shape, and it is what makes the stated bound hold.

**One reversal of a previously resolved finding, recorded explicitly.** Revision 7's S1e made a
diagnostic slot-reservation failure return a typed non-accepted result, which let a diagnostic failure
reject a valid event. That resolved `finding_1787288480_127520`. The decision reclassifies the age as
non-authoritative, so that rule is now wrong. S1f withdraws it: a diagnostic failure never changes
acceptance, the sample becomes indeterminate, and `age_sample_failures` counts it. AC16 is rewritten to
prove the reversal.

## 0. Response to Plan Review `review_1787289501_106968` (revision 9)

| Finding | Severity | Response |
| --- | --- | --- |
| `finding_1787289501_918216` — producer retirement cannot both prune the registry and avoid its lock | blocker | **Accepted. This was a self-contradiction I introduced in revisions 7 and 8.** S1b and S1c told `retire_holder_locked` to remove the registry entry, while S1d said the registry lock is taken only at admission and status read, and AC15 requires retirement to complete while another thread holds that lock. Those cannot all hold. New section S1h defines one design that satisfies both: **retirement never touches the registry.** It marks the cell dead through an atomic flag, and the next registry write — which is admission, already holding the write lock and never on an event path — prunes dead entries. The bound is real because a new producer identity can only appear through admission, so every new identity forces a prune first. This **removes** the retirement-side registry requirement rather than adding machinery. |
| `finding_1787289501_304418` — S1f cites replacement preview as direct commit validation | high | **Accepted and fixed.** Verified: `preview_package_replacement` is called at `src/package_event_router.rs:367`, which is inside `try_replace_package_generation` (opens at `:352`), not inside `try_commit_package_generation` (`:333-349`). Direct commit validates contract ownership and then calls `commit_package_generation_locked`, relying on sequential `subscribe_locked` plus `restore_admission`. The S1f **design** still holds, because the commit point sits after the last fallible step, but my stated **reason** was false. S1f now gives the correct per-entry-point justification, and AC17 exercises both entry points separately because their failure paths differ. |

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
- Per-queue `QueueAgeMetric` cells, which carry the **bounded diagnostic** oldest age rather than an
  authoritative value (see S1a through S1d).

**S1a. Oldest age is a bounded diagnostic observation, not authoritative queue state.**

Human decision of record: `question_1787290055_403092`. Revisions 2 through 9 assumed the oldest age had
to be exact and linearizable. That assumption, which I introduced rather than the ticket, forced lock-free
retirement, deferred registry pruning, cross-atomic ordering, and list-versus-registry lifetime
separation, and it produced most of the twenty-two findings this plan has drawn. The decision reclassifies
the signal and the design collapses accordingly.

**The contract, exactly as decided:**

- Oldest age is a **bounded diagnostic observation**. It is not authoritative queue state and needs no
  exact linearizable snapshot.
- **Staleness bound: at most one successful mutation of that exact queue.**
- **A queue whose observed count is zero reports no usable age.** Not a zero age, not a stale age.
- The reader uses a **bounded non-blocking consistency read**: count, then age, then count again, with **at
  most one retry**. If the count changes across the bracket, the sample is **indeterminate** and must be
  published as an explicit indeterminate marker. **An indeterminate sample must never become a false age.**
- The reader must **never lock, wait, spin, or allocate per sample**, and must never publish an age from a
  **retired or reused producer generation**.
- A later producer generation **starts with a fresh or explicitly reset metric cell**.
- **Authoritative signals are unchanged**: queue count, shed, gap, latency, and the four timeout counters
  keep their existing contracts and are not weakened by this reclassification.
- Campaign usage, recorded so the consumer ticket inherits it: age is valid for **trend, expiry, and
  stuck-queue diagnostics**. A single indeterminate or one-event-stale sample **must not be treated as
  authoritative failure evidence**.

**S1b. The metric cell and its two-phase version protocol.**

```rust
struct QueueAgeMetric {
    version: AtomicU64,       // even = stable, odd = write in progress
    count: AtomicU64,         // observed queue count at the last completed mutation
    oldest_nanos: AtomicU64,  // u64::MAX = empty
    gate: AtomicU64,          // surviving prior-generation holders; > 0 means indeterminate (S1c)
    generation: u64,          // immutable: the producer generation this cell belongs to
    invalid: AtomicBool,      // latched by a diagnostic failure; see S1f
}
```

`gate` lives **on the cell, not on `ProducerOccupancy`**, corrected in revision 12 for
`finding_1787291409_621659`. Revision 11 put the prior-holder count inside `RouterInner`, which S2 forbids
the saturation snapshot from touching, so the publication gate was unreadable by the lock-free reader that
depends on it. It is now a cell field written inside the two-phase bracket like every other.

**Two deliberate tightenings of the decided protocol, both flagged rather than substituted silently.**

1. The decision names a `count`-age-`count` bracket. A raw count is not monotonic: a `5 → 6 → 5` sequence
   would pass a naive bracket while the sample is two mutations stale, exceeding the stated one-mutation
   bound. The bracket compares a version stamp instead, while the sample still carries `count`.
2. Revision 10 used a **trailing-only** stamp — store the fields, then bump once. Plan Review
   `finding_1787290799_815700` showed that is unsound: a reader can load `v1`, read fields while the
   writer is mid-store, and load an unchanged `v2`, accepting a **mixed sample**. Across a generation
   change that pairs a new generation with an old age, which the decision forbids outright. The protocol is
   therefore **two-phase odd/even**, which marks the update in progress *before* any field changes.

**Writer rule**, executed while the writer already holds the router or mailbox lock it holds today. The
opening RMW is **`AcqRel`, not `Release`** — corrected in revision 12 for
`finding_1787291409_211318`. A `Release` RMW stops earlier accesses moving *after* it; it does **not** stop
later relaxed stores moving *before* it, so a reader could observe changed fields while still seeing the
old even version. The acquire half of `AcqRel` is what pins the field stores after the odd transition.

```text
version.fetch_add(1, AcqRel)       // now odd: update in progress; field stores cannot move before this
store count, oldest_nanos, gate    // relaxed
store invalid                      // relaxed, when this update latches it
version.fetch_add(1, Release)      // now even: update complete
```

**Every mutable field travels inside this bracket**, including `invalid` and the generation gate of S1c.
Nothing the reader consumes is written outside a bracket, so AC21's requirement that every later sample be
indeterminate is carried by the same protocol rather than a second one.

**Reader rule**, holding no lock, at most one retry:

```text
v1 = version.load(Acquire)
if v1 is odd            -> retry once, then indeterminate
sample = (count, oldest_nanos, gate, invalid)   // relaxed
atomic::fence(Acquire)                          // pins the field loads before the v2 read
v2 = version.load(Relaxed)
if v1 != v2             -> retry once, then indeterminate
if invalid              -> indeterminate
if gate > 0             -> indeterminate            // prior generation still draining, S1c
if count == 0           -> no usable age
otherwise               -> usable age, tagged with this cell's immutable generation
```

This is the canonical seqlock reader shape: an acquiring first load, relaxed field loads, an `Acquire`
fence, then the second version read. AC19 adds a `loom` model check alongside the deterministic
interleavings, because scheduling tests alone cannot prove an ordering claim.

`generation` is **immutable per cell**, not an atomic that is reset in place. A cell belongs to exactly one
producer generation for its whole life, which is what makes "never publish an age from a retired or reused
generation" structural rather than a race to win. See S1c.

**S1c. A generation gets its own cell, and reports indeterminate until the previous generation drains.**

Revision 10 reset one shared cell in place and bumped a `generation` atomic. Plan Review
`finding_1787290799_555137` showed that is unsound. Existing Hub rules keep old-generation holders alive
across unload and replacement, per
`[[admitted event holders survive producer unload until Core completion]]`, so one owner age list can hold
old and new slots at the same time. If an old slot sits at the head, updating the reset cell from that head
publishes an age from the **retired** generation, which the decision forbids.

Revision 11 removes the shared-cell reuse entirely:

- **A cell is created fresh for each producer generation at admission** and is never reset and reused. Its
  `generation` field is immutable, so a sample can always be attributed to exactly one generation.
- **The previous generation's cell is marked retired on the same control path** that admits the new one.
  A retired cell reports **no usable age** and is never written again.
- **The new generation reports `indeterminate` until every prior-generation holder has retired.** The gate
  lives in `QueueAgeMetric.gate` on the cell, not on `ProducerOccupancy`, precisely so the lock-free
  snapshot can read it (S1b). While `gate` is above zero the reader reports indeterminate; at zero the
  generation reports its own age normally.

  **The gate counts *all* prior generations, and only the current cell is ever decremented** — corrected in
  revision 13 for `finding_1787292035_541858`. Revision 12 said a retiring prior envelope decrements "the
  successor" cell, which is ambiguous once three generations overlap. With N replaced by N+1 and then N+2
  before N drains, N+1 is already retired and write-closed, so decrementing the immediate successor would
  leave N+2's gate permanently above zero and N+2 would never publish an age. The exact rules:

  - `ProducerOccupancy` carries `outstanding_prior: usize`, the number of live envelopes whose generation
    is not the current one.
  - **At replacement**, `outstanding_prior += live(previous_current_list)`, and the **new current cell's**
    `gate` is seeded to that updated total — every live prior envelope across **all** prior generations,
    not just the immediately preceding one.
  - **At retirement**, when `envelope.producer_age_ref.generation != current_generation`, decrement
    `outstanding_prior` and the **current** cell's `gate`. Never a retired cell: retired cells are
    write-closed and receive nothing.
  - Both operations are `O(1)`, and the seed reads a single running total rather than summing over prior
    lists.
- **No old timestamp can ever update the new cell**, because old slots belong to the old generation's list
  and the old cell, and the old cell is retired and write-closed.

**Envelope carries generation-specific list identity**, corrected in revision 12 for
`finding_1787291409_910821`. Revision 11 claimed old holders "retire against their own list" while
`Envelope` carried only a bare `producer_slot: u32` and the router held one age-list map keyed by owner. A
slot number alone cannot select the old generation's list after replacement, so the behaviour AC14 and
AC20 demanded was not implementable from the state shape. The corrected shape:

```rust
struct ProducerAgeRef { generation: u64, slot: u32 }   // on Envelope, replaces producer_slot
```

- Producer age lists are keyed by **`(owner, generation)`**, not by owner alone.
- Each list carries a `live: usize` count of envelopes still referencing it.
- `retire_holder_locked` resolves `(generation, slot)` to the exact list that admitted the envelope,
  unlinks the slot, decrements that list's `live`, and — when the retiring envelope belonged to a prior
  generation — decrements `outstanding_prior` and the **current** cell's `gate`, never a retired
  predecessor's. See the gate rules above.
- **Bounded cleanup**: when a non-current generation's `live` reaches zero, its list and its retired cell
  are dropped at that point. The current generation's list is never dropped while it is current.
- The number of live lists per owner is therefore bounded by one current generation plus those prior
  generations that still have unretired envelopes, and every prior list is guaranteed to drain because
  each of its envelopes retires exactly once.

Only the **publication** of an age is generation-scoped; occupancy and retirement accounting are unchanged.

**S1d. Registry membership is explicit lifecycle state, not an `Arc` strong count or an empty queue.**

Revision 10 said admission prunes cells that are "retired and unreferenced". Plan Review
`finding_1787290799_270331` showed that predicate cannot work: the registry itself owns one `Arc`,
`inner.consumers` entries are never removed and keep their own `Arc`, so a retired consumer cell stays
referenced forever. Emptiness is no better, because a live queue that happens to be empty is
indistinguishable from a retired one.

Revision 11 makes membership explicit and independent of both signals:

- The registry stores `{ Arc<QueueAgeMetric>, retired: bool }` keyed by identity **and generation**.
- `retired` is set **only on a control path** that knows the identity ended: `apply_unload`,
  `unsubscribe`, connection cleanup, or the admission of a replacing generation. Nothing infers retirement
  from a strong count or from `count == 0`.
- **Admission removes retired registry entries regardless of remaining `Arc` holders.** A retained
  `ConsumerQueue` may keep its direct `Arc` to a removed cell; that cell is write-closed and reports no
  usable age, so the retained handle is harmless.
- **A later admission for the same key binds the container to the new generation's cell** and inserts that
  cell in the registry. The container's stale `Arc` is replaced at that point, not before.
- No event path and no retirement path touches the registry lock. Admission and status read remain the
  only holders.

Bound: the registry holds one entry per live identity-generation plus those retired since the last
admission, and admission is the only insert path, so a sweep always precedes growth.

**S1e. Producer oldest-age source.**

Producers have no queue head: `ProducerOccupancy` (`src/package_event_router.rs:197-200`) holds only
`events` and `bytes`, and `retire_holder_locked` (`:1338-1352`) removes envelopes from an unordered
`HashMap` in arbitrary order. The intrusive age list from revision 6 remains the cheap way to know the new
oldest after an out-of-order retirement, and it survived review on its own merits:

```rust
struct ProducerAgeSlot { nanos: u64, prev: u32, next: u32 }
struct ProducerAgeList { slots: Box<[ProducerAgeSlot]>, head: u32, tail: u32, free: u32 }
```

`Envelope` carries `producer_age_ref: ProducerAgeRef { generation, slot }` (S1c). Push pops the free head
and links at tail; remove unlinks through
`prev`/`next` and returns the slot; oldest reads `slots[head].nanos`. All three are strict `O(1)` with zero
allocator calls. The list is allocated at the S1g admission commit point and never on an event path.

Consumer queues and client mailboxes read their existing `VecDeque` front in `O(1)` and need no new
structure.

**S1f. A diagnostic failure latches a persistent invalid state and never changes production behaviour.**

Two rules, and revision 10 only had the second.

Revision 7 made a diagnostic slot-reservation failure return a typed non-accepted result, so a diagnostic
failure could reject a valid event. The human decision reclassifies the age as non-authoritative, so that
rule is wrong and stays withdrawn. This reverses the resolution rationale of
`finding_1787288480_127520`, which the decision supersedes.

But revision 10 then said only "the sample becomes indeterminate", with nowhere to record it. Plan Review
`finding_1787290799_691196` showed why that fails: the accepted envelope has **no producer slot**, so the
age list is permanently incomplete, later updates look perfectly stable, and the cell would publish a
usable age computed from a list that is missing an entry. A counter records the incident but cannot make
later samples indeterminate.

Revision 11 adds the missing state:

- `QueueAgeMetric.invalid: AtomicBool` is **latched** on any diagnostic failure: a slot-reservation
  failure, or a write that finds no cell for the identity.
- **Once latched, every subsequent read of that cell reports indeterminate**, regardless of how stable the
  version bracket looks. The reader checks `invalid` inside the bracket (S1b).
- **The latch clears only when that identity-generation ends.** Because a cell is never reset in place
  (S1c), clearing means the next generation gets a fresh cell with `invalid == false`. There is no
  in-place un-latch to race against.
- **A missing cell is represented explicitly in the public snapshot** as an indeterminate entry for that
  identity, never as an omitted row that a reader could mistake for "no queue".
- `age_sample_failures` still counts incidents, so the breach is visible as well as safe.
- Acceptance is untouched in every case: the event is admitted or shed on its existing production criteria
  alone.

**S1g. Diagnostic state is created only after a whole admission succeeds.**

`commit_package_generation_locked` (`:1041-1066`) snapshots through `snapshot_admission` and restores on a
failed later subscription, and `AdmissionSnapshot` (`:946-967`) carries only `contracts`, `subscriptions`,
`subscriptions_per_plugin`, and `package_generation`. Creating diagnostic state mid-batch would survive
that restore. Diagnostic creation therefore happens at a **single commit point after the last fallible
step**, so rollback has nothing to undo and `AdmissionSnapshot` keeps its exact shape.

Correct per-entry-point inventory, fixing revision 8's misattribution:

- **`try_commit_package_generation` (`:333-349`)** validates contract ownership then calls
  `commit_package_generation_locked` directly. It has **no** preview and relies on sequential
  `subscribe_locked` with `restore_admission`.
- **`try_replace_package_generation` (`:352`)** calls `preview_package_replacement` (`:367`) before
  `apply_unload` and the same commit. The `:367` call site is inside **this** function, not the direct
  commit.
- **`try_register_contracts` (`:300`)** validates the whole contract batch first.

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

**S6a. The public age observation DTO (new in revision 13 for `finding_1787292036_111963`).**

Revision 12 defined only the top-level `DaemonStatus.observability` field. The three states the human
decision created — usable, no usable age, and explicit indeterminate — had no public shape, so AC10 could
not prove wire compatibility or generated optionality, and Implement would have had to settle a public
client-contract question that `[[botster-hub-client-playbook]]` assigns to Plan. Settled here.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonQueueAgeObservation {
    pub kind: DaemonQueueKind,
    /// Producer owner, consumer plugin key, or client connection id.
    pub identity: String,
    /// Present only for `Producer`; identifies which generation the sample belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_generation: Option<u64>,
    pub state: DaemonQueueAgeState,
    /// Present only when `state == Usable`. Microseconds, matching the owner-turn fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_age_us: Option<u64>,
    /// The queue count observed in the same bracket as the age.
    pub queue_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DaemonQueueKind { Producer, Consumer, ClientMailbox }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DaemonQueueAgeState {
    /// A sample the reader validated; `oldest_age_us` is present.
    Usable,
    /// The queue was observed with `count == 0`. Not a zero age.
    Empty,
    /// The bracket was unstable, the cell was latched invalid, the generation gate was open, or the
    /// cell was missing. Never a value.
    Indeterminate,
    /// Forward tolerance: an older client deserializing a newer state lands here.
    #[serde(other)]
    Unknown,
}
```

`DaemonObservabilityCounters` gains
`#[serde(default, skip_serializing_if = "Vec::is_empty")] pub queue_ages: Vec<DaemonQueueAgeObservation>`.

**Rules that travel with the contract:**

- **Three states are distinct and none substitutes for another.** `Empty` is not a zero age;
  `Indeterminate` is not an omitted row. A missing cell is published as an `Indeterminate` row for that
  identity, per S1f.
- **`Unknown` is treated exactly as `Indeterminate`** by any consumer. `#[non_exhaustive]` alone does not
  make serde tolerant of an unseen variant, so the `#[serde(other)]` arm is what actually keeps an older
  client from failing to deserialize a newer Hub. This is the forward-evolution rule.
- **Units are microseconds**, matching `last_owner_turn_us` and the ready-wait fields, so no reader has to
  track two units in one payload.
- **`oldest_age_us` and `producer_generation` are `Option`**, so generated TypeScript must emit
  `oldest_age_us?: number` and `producer_generation?: number`, per
  `[[generated typescript dtos must encode serde field optionality]]`. Generated TypeScript for `state` is
  the snake_case union plus a permissive arm for unknown values.
- **Both structs and both enums are `#[non_exhaustive]`**, so later additive fields and variants cost
  external Rust consumers nothing beyond the one-time boundary already accepted in S6.

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

**A8 — rewritten again in revision 12.** The age list is addressed by the `(generation, slot)` pair carried
on `Envelope.producer_age_ref`, so envelope-id monotonicity is irrelevant and a late retirement resolves
its own generation's list. Two narrower checks remain for
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
| `src/event_plane_counters.rs` (new) | `EventPlaneCounters`, fixed shed-by-reason array, fixed-bucket histograms, `QueueAgeMetric` with the two-phase odd/even `version`, `gate`, immutable `generation`, and `invalid` latch, the bounded consistency read with its `AcqRel` opening RMW and `Acquire` fence, the enumeration-only registry keyed by identity and generation with explicit retirement, and the snapshot type including its indeterminate representation |
| `src/lib.rs` | register the new module and re-export the snapshot type |
| `Cargo.toml` and `Cargo.lock` | add the `[target.'cfg(loom)'.dev-dependencies]` `loom` entry for AC19 case 0, pinned by Implement and outside the normal build |
| `src/package_event_router.rs` | shed by typed reason, admission and delivery attempts, latencies, T2, producer age lists keyed by `(owner, generation)` and created at the S1g admission commit point, `Envelope.producer_age_ref: ProducerAgeRef { generation, slot }` replacing `producer_slot`, a per-list `live` count with cleanup when a non-current generation drains, a **fresh** `Arc<QueueAgeMetric>` per generation on the producer entry and consumer queue with the predecessor **retired and write-closed** (never reset in place), `gate` seeded at replacement and decremented in `retire_holder_locked` inside the two-phase bracket, registry retirement recorded explicitly at `apply_unload`, `unsubscribe`, and connection cleanup with an admission-time prune and rebind, and a diagnostic failure that latches `invalid` without changing acceptance |
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
- **R10 (added to the plan in revision 10).** The sibling dependency re-registration required before
  Implement could be forgotten, which would let this ticket write conformance revision 46 and package
  0.1.41 against an unmerged sibling that still owns 45 and 0.1.40. Section 14 names the exact call, and
  gate evidence repeats it. **I cited R10 in revision 8 and revision 9 gate evidence while it was absent
  from the plan file; this verification pass caught that, and it is the same class of error as the AC11
  claim recorded in section 0z.**
- **R11 (new in revision 6).** A diagnostic structure can silently lose a live entry when its own
  occupancy accounting diverges from `producer.events`. That is exactly how the revision 4 tombstone ring
  failed review. AC13 is the direct control and must be shown red against that ring.
- **R12 (new in revision 6).** Diagnostic storage can allocate on the first event for an owner if it is
  attached to a lazily created map entry. `try_ingress` creates `ProducerOccupancy` before the shed check
  at `src/package_event_router.rs:473-483`, so the age list is deliberately kept in a separate map that is
  populated at contract admission. AC6 part 1 is the control.
- **R2. A second lock replaces the first.** Revision 6 had event writers take the registry `RwLock`, which
  would have let a status read or an admission delay an accepted event. Each `QueueAgeMetric` is shared by
  `Arc`, so event and retirement paths update atomics through a direct handle and **no event path and no
  retirement path acquires the registry lock**. AC4 covers the status read; AC15 is the contention control.
- **R13 (rewritten in revision 11).** A retired or reused generation can publish an age that belongs to a
  previous generation. Revision 10's reset-in-place did not prevent this while old holders survived unload.
  S1c gives each generation its own cell with an immutable `generation` field, retires and write-closes the
  predecessor, keys age lists by `(owner, generation)` so a late retirement resolves its own list, and
  reports indeterminate until the cell's `gate` reaches zero. AC14, AC18, and AC20 are the controls.
- **R18 (rewritten in revision 11).** A non-monotonic version stamp admits ABA: a `5 -> 6 -> 5` count
  sequence would pass a raw count bracket while the sample is two mutations stale. S1b brackets on the
  two-phase odd/even `version`, which advances by four across that sequence, and AC19 case 5 is the control.
- **R20 (new in revision 11).** A trailing-only version stamp accepts a mixed sample, because a reader can
  bracket a writer that is mid-store. S1b uses a two-phase odd/even version that marks the update in
  progress before any field changes. AC19 case 3 is the control.
- **R21 (new in revision 11).** One shared cell reset in place cannot separate generations while old
  holders survive unload, so an old head age can be published as the new generation's. S1c gives each
  generation its own immutable-generation cell and reports indeterminate until prior holders drain.
  AC20 is the control.
- **R23 (new in revision 13).** A relative pointer like "the successor" breaks once more than two instances
  overlap. The gate rule now names the **current** cell absolutely and counts all prior generations.
  AC20b is the control.
- **R24 (new in revision 13).** `#[non_exhaustive]` does not make serde tolerant of an unknown enum
  variant; only a `#[serde(other)]` arm does. Without it a newer Hub state would fail an older client's
  deserialization outright. AC10 asserts the unknown-state case.
- **R25 (new in revision 13).** An acceptance gate that names a tool absent from every manifest is not
  executable. AC19 case 0 now carries its dependency, its command, and an explicit fallback that reports
  the ordering claim as unproven rather than dropping it.
- **R22 (new in revision 11).** A diagnostic failure with no persistent state lets later stable-looking
  samples publish an age computed from an incomplete list. S1f latches `invalid` until the generation ends.
  AC21 is the control.
- **R19 (new in revision 10).** An indeterminate sample could be rendered as a real value by a careless
  status projection, which would be worse than omitting it. S1a requires an explicit indeterminate marker
  and AC19 asserts a false age is never published.
- **R15 (new in revision 8).** New state added at admission is invisible to an existing rollback snapshot.
  `AdmissionSnapshot` covers four maps only, so any diagnostic map created mid-batch would survive a
  restore. S1f avoids this by committing diagnostic state only after the whole batch succeeds. AC17 is the
  control.
- **R17 (rewritten in revision 12).** A cleanup rule and a no-lock rule can contradict each other silently
  when they live in different sections. Under the current design a lingering registry entry is harmless
  because a **retired, write-closed** cell reports no usable age, so pruning is a memory bound only.
  Retirement is explicit lifecycle state per S1d, never inferred. AC11 and AC15 assert it from both
  directions.
- **R16 (rewritten in revision 11).** A long-lived container that outlives its identity can retain a stale
  shared handle. `inner.consumers` entries are never removed, so the predecessor cell is retired and
  write-closed and the container is rebound to the new generation's cell at the next admission. A retained
  handle therefore reports no usable age rather than a stale one. AC11 and AC18 are the controls.
- **R14 (rewritten in revision 10).** A defensive skip degrades observability silently. The decided model
  keeps that concern without letting diagnostics affect production: a failure yields an **explicit
  indeterminate sample** plus a counted `age_sample_failures`, and never a false age, while acceptance is
  untouched. AC16 and AC19 are the controls.
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

- **AC1 — public readability (extended in revision 13 for the S6a DTO).** One test drives
  `DaemonRequest::Status` through the production daemon path and asserts every signal is present and
  non-absent: queue count and bytes, oldest age per producer queue, per consumer queue, and per client
  mailbox, admission latency, delivery latency, shed by typed reason, gap, resync, pressure, T1 through T4
  as four distinct values, owner-turn duration, and ready-operation wait. Beyond presence, assert each age
  row carries its `kind` and `identity`, that producer rows carry `producer_generation`, and that a
  producer row under an open generation gate reports `indeterminate` rather than being omitted.
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
  5. `RUSTFLAGS="--cfg loom" cargo test --lib queue_age_model` for AC19 case 0. It is a separate command
     because the `cfg(loom)` dev-dependency is deliberately outside the normal build, so `./test.sh
     --locked` neither compiles nor runs it.
- **AC10 — client DTO proof (new in revision 2).** Four separate proofs, per
  `[[botster-hub-client-playbook]]`'s gate to separate serde wire proof from downstream source proof:
  1. **Wire proof.** A serde test shows an old-shaped `DaemonStatus` JSON without the new key still
     deserializes, and that an empty `observability` value is omitted from the serialized frame.
     **Extended in revision 13 for the S6a DTO**: assert the exact wire form of all three age states — a
     `usable` row carrying `oldest_age_us` and, for producers, `producer_generation`; an `empty` row with
     both fields **absent** rather than zero; and an `indeterminate` row with both absent. Assert a
     missing cell appears as an `indeterminate` row rather than an omitted one. Assert that an unknown
     future `state` string deserializes to `Unknown` through the `#[serde(other)]` arm rather than
     failing, which is the forward-evolution rule that `#[non_exhaustive]` alone does not provide.
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
- **AC11 — retired cells report no usable age, and the registry stays bounded, red first (rewritten in
  revision 10, extended in revision 11 for `finding_1787290799_270331`).** Add a case that retires a
  consumer identity whose `ConsumerQueue` entry is **never removed** and therefore keeps its own `Arc`, and
  assert admission still removes that retired registry entry, that the retained handle reports no usable
  age, and that a later admission binds the container to the new generation's cell. Assert retirement is
  read from explicit lifecycle state, never inferred from an `Arc` strong count or from `count == 0`; a
  **live but empty** queue must not be pruned. The remaining assertions are unchanged: Revision 9 claimed this was tightened; it was not, and its body still described
  immediate removal. Rewritten now to match the decided model. Assert three things. First, after package
  unload, client unsubscribe, and connection cleanup, the retired cell reports **no usable age** because
  its `count` is zero, and never a stale value. Second, run a reconnect-churn loop that creates and
  destroys `N` identities with no intervening admission, and assert the registry holds at most
  `live + retired_since_last_admission`, a number the test computes exactly from `N` rather than an
  approximation. Third, assert that the next successful admission prunes every prior retired entry and the
  registry returns to exactly the live-identity count. The test must fail when the prune site is omitted.
- **AC19 — two-phase bounded consistency read, red first (new in revision 10, rewritten in revision 11).**
  The direct control for the decided protocol and for `finding_1787290799_815700`. Assert, with
  **deterministic interleavings** rather than timing luck:
  0. A **`loom` model check** of the writer and reader protocol, which is the only way to substantiate an
     ordering claim; deterministic scheduling tests cannot. It must fail against a `Release` opening RMW.
     **Scope and command, added in revision 13 for `finding_1787292036_587759`** — revision 12 named this
     gate without making it executable. `loom` is absent from `Cargo.toml`, every crate manifest, and
     `Cargo.lock`. Implement therefore adds a **cfg-gated dev-dependency** so the normal build and the
     normal test run are untouched:

     ```toml
     [target.'cfg(loom)'.dev-dependencies]
     loom = "<current release, pinned by Implement>"
     ```

     and runs it as `RUSTFLAGS="--cfg loom" cargo test --lib queue_age_model`, listed under AC9 as a
     separate command rather than folded into `./test.sh --locked`. `Cargo.toml` and `Cargo.lock` are
     added to the affected-file table. **I did not verify the current `loom` version number from the
     registry**, so Implement pins the current release and records the exact version in its report; if
     `loom` cannot be added under the locked-build policy, Implement must report that and fall back to the
     deterministic interleavings alone, saying plainly that the ordering claim is then unproven rather
     than silently dropping case 0.
  1. A stable even version with `count > 0` and `gate == 0` yields a usable age tagged with that cell's
     generation.
  2. `count == 0` yields **no usable age**, never a zero age.
  3. A writer **paused mid-update**, with the version left odd, yields **indeterminate** after exactly one
     retry — never a mixed sample. This is the case revision 10's trailing-only stamp accepted.
  4. A completed mutation between the bracket loads yields indeterminate after one retry.
  5. The ABA case: two mutations returning `count` to its original value still reports indeterminate,
     because the version advanced by four.
  6. `invalid == true` yields indeterminate regardless of how stable the bracket looks.
  7. The reader takes no lock, retries at most once, and allocates nothing per sample.
  Run interleavings **before, during, and after a generation change**. Implement must show AC19 red against
  the revision 10 trailing-only stamp and red against a naive count-only bracket.
- **AC20b — repeated replacement drains the current gate, red first (new in revision 13).** The control for
  `finding_1787292035_541858`, which AC20's single replacement could not detect. Admit generation N, emit
  events Core admits as Background holders, replace with N+1 while N is still live, then replace with N+2
  **before N drains**. Assert that N+2's `gate` was seeded from the live envelopes of **both** N and N+1,
  that a late N retirement decrements **N+2's** gate and not retired N+1's, that `outstanding_prior` and
  the current gate stay equal, and that N+2 begins publishing its own age exactly when the last prior
  envelope from either generation retires. Interleave the late retirements across the two replacements.
  Implement must show this red against the revision 12 "decrement the successor" rule, under which N+2's
  gate never reaches zero and N+2 never publishes an age.
- **AC20 — no age from a prior generation, red first (new in revision 11).** The control for
  `finding_1787290799_555137`. Admit generation N, emit events Core admits as Background holders, then
  replace with generation N+1 while old holders remain live. Assert that generation N+1 reports
  **indeterminate** while its cell's `gate > 0`, **observed through the public snapshot path** rather than
  through `RouterInner`, that no sample is ever attributed to generation N after its cell is retired, that
  the old holders' `producer_age_ref` values resolve to generation N's own list and retire correctly
  against it, that generation N's list is dropped when its `live` count reaches zero, and that N+1 begins
  reporting its own age only once the last old holder retires.
  Implement must show this red against revision 10's reset-in-place cell, which publishes an old-generation
  head age to the new generation.
- **AC21 — a diagnostic failure latches indeterminate, red first (new in revision 11).** The control for
  `finding_1787290799_691196`. Force a slot-reservation failure so an accepted envelope has no producer
  slot. Assert the accept-or-shed outcome is identical to the uninjected run, that `age_sample_failures`
  incremented, and then assert **every subsequent sample of that cell reports indeterminate** across later
  accepted events and later retirements, even though the version bracket is stable. Assert the latch clears
  only by the arrival of a new generation with a fresh cell, and that a missing cell appears in the public
  snapshot as an explicit indeterminate entry rather than an omitted row. Implement must show this red
  against revision 10, which had no field to hold the state.
- **AC17 — admission rollback leaves no diagnostic residue, red first (new in revision 8, split in
  revision 9).** Run the case **separately for `try_commit_package_generation` and
  `try_replace_package_generation`**, because their failure paths differ: direct commit has no preview and
  fails inside sequential subscription admission, while replacement can also fail at
  `preview_package_replacement` before `apply_unload`. In each case, submit a package generation whose
  first subscription is valid and whose later subscription is invalid. Capture every admission map and
  every diagnostic map before the call, and assert each equals its exact pre-call state afterwards.
  Implement must show this red against a variant that creates diagnostic state before the batch completes.
- **AC18 — a reused generation starts fresh, red first (rewritten in revision 10).** Subscribe, queue a
  copy, then retire the identity through unsubscribe or unload. Assert the cell reports no usable age.
  Admit the **next** generation for the same key, queue a first copy, and assert the reported age belongs
  to the new generation, that `generation` advanced, and that no sample from the retired generation can be
  published. Implement must show this red against a variant that reuses the cell without resetting it.
- **AC14 — late completion after unload and replacement, red first (new in revision 7, rewritten in
  revision 10).** Admit a producer contract, emit an event that Core admits as a Background holder, unload
  the producer, admit the next generation, then complete the **old** holder late. Assert the old holder's
  `producer_age_ref` still resolves to the generation-N list that admitted it, that its retirement
  decrements the correct occupancy and that generation's `gate`, that
  it does not disturb a slot owned by the new generation, and that **no age from the retired generation is
  ever published**. Assert the new generation's age is reported from its own fresh cell.
- **AC15 — no event or retirement path waits on the diagnostic registry, red first (new in revision 7).**
  Hold the snapshot registry lock on one thread. On another thread run an accepted emit, a shed emit, and a
  retirement including a final identity retirement. Assert all three complete without waiting for the
  registry lock and without returning `ShedBusy` for that reason.
- **AC16 — a diagnostic failure never changes acceptance, red first (rewritten in revision 10; the
  persistence half moved to AC21 in revision 11).** Force a slot-reservation failure and a missing-cell case through a test seam. Assert the
  event's accept-or-shed outcome is **identical** to the same event without the injected failure, that the
  affected sample is reported **indeterminate** rather than as a false age, and that `age_sample_failures`
  incremented. Implement must show this red against revision 7's behaviour, which returned a typed
  non-accepted result and therefore let a diagnostic failure reject a valid event.
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
12. **A defensive skip is an observability outage under load, but the fix must not let diagnostics change
    production.** Revision 7 made a diagnostic failure reject a valid event; human decision
    `question_1787290055_403092` reversed that. The durable rule is an explicit indeterminate sample plus a
    counted failure, never a false value and never a changed accept-or-shed outcome.
13. **New admission-time state must be added to the existing rollback snapshot, or created only after the
    batch commits.** `AdmissionSnapshot` covers four maps, so anything else created mid-batch survives a
    restore silently.
14. **A container that outlives its identity retains stale shared handles.** Because `inner.consumers`
    entries are never removed, the shared cell must be reset at retirement and generation-stamped on
    reuse, so a retained handle reports no usable age instead of a stale one.
15. **Appending a corrected section does not retract the original.** Revisions 7 left superseded S1a
    instructions active beside their replacements, which is an implementation hazard rather than a
    documentation nit. A revision that changes a rule must rewrite the rule, not only add the new one.
16. **A deferred-cleanup rule must name the site that actually performs the removal.** Writing "remove at
    retirement" in one section and "never lock at retirement" in another produced a design that could not
    be implemented. Pruning at the next admission works only because a new identity cannot appear without
    an admission, which is what makes the bound real rather than asserted.
17. **Cite the exact enclosing function, not the nearest line number.** `preview_package_replacement` at
    `:367` sits inside `try_replace_package_generation`, not the direct commit entry point, and the wrong
    attribution produced a true conclusion from a false premise.
18. **A requirement the planner invented can cost more than the requirement the ticket stated.** I assumed
    oldest age had to be exact and linearizable. The ticket never said so. That assumption produced most of
    twenty-two findings across seven reviews; reclassifying the signal as a bounded diagnostic observation
    roughly halved the design. Question the strength of a self-imposed guarantee before engineering around it.
19. **A verification method that reads the wrong artifact is worse than none.** I verified plan edits by
    grepping for strings that other, successful edits had written, so silently discarded edits looked
    applied, and I then asserted in durable gate evidence that an acceptance check had been tightened when
    it had not. Verify the committed file, item by item, before citing it.
20. **A non-monotonic version stamp admits ABA.** Bracketing a sample on a raw queue count passes when the
    count returns to its original value across two mutations. A monotonic mutation stamp is what makes a
    stated staleness bound actually hold.
21. **A trailing-only version stamp is not a seqlock.** Marking completion after the writes does not stop
    a reader from bracketing a writer that is mid-store. The in-progress mark must precede the first field
    change, which is what odd/even versioning gives.
22. **Reset-in-place cannot separate generations when old holders outlive the reset.** A cell whose
    generation field is mutable can be paired with a predecessor's data. An immutable per-generation cell
    makes the attribution structural instead of a race to win.
23. **A diagnostic that can fail needs a persistent failure state, not just a counter.** A counter records
    that something went wrong; only a latch stops later stable-looking samples from publishing a value
    derived from the broken state.
24. **Lifecycle membership must be recorded, not inferred.** Neither an `Arc` strong count nor an empty
    queue distinguishes retired from live, so retirement belongs on the control path that knows.
25. **Publish the artifact after the final commit.** I created an artifact, then committed a further fix,
    leaving durable evidence that described bytes the reviewer never saw.
26. **`Release` on an opening RMW does not fence what follows it.** A seqlock's in-progress mark needs
    acquire semantics too, or later relaxed stores can float above it and a reader can see changed fields
    under an unchanged version.
27. **A published gate must live where its reader can reach it.** Putting a publication condition inside a
    structure the lock-free reader is forbidden to touch makes the condition unenforceable.
28. **Prose cannot grant a capability the state shape lacks.** "Old holders retire against their own list"
    was false while the envelope carried only a slot number; the sentence described behaviour no field
    could support.
29. **A sweep keyed on remembered strings finds only what you remember.** Report the literal command and
    its literal output; a summarised count is an assertion, not a verification.
30. **Correcting a stale in-repository assumption** — an in-repository workspace member can still be an
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
