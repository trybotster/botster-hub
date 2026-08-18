# Hub: make session snapshot paging deterministic

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, confirmed against the admitted spawn-target list.
- Pipeline ticket: `ticket_1786912570_127968`.
- Run: `run_1787092386_910379` (restart of `run_1787087218_390370`; see Revision 3).
- Pipeline: Botster Stack Delivery.
- Assigned worktree: the pipeline-created Hub worktree for this ticket, branch `project-pipelines/ticket_1786912570_127968`.
- Base: `origin/main` at `674d2df`. The base contains the closed dependency fix `6d69028` (`ticket_1786913892_208903`, WebRTC write-budget sibling continuation).
- This ticket replaces only the snapshot-paging slice of superseded `ticket_1786875812_242946`. Start from current main. Do not cherry-pick that branch.

`teardown_class_applies`: no. This ticket changes snapshot page assembly inside entity delivery. It does not touch WebRTC or peer lifecycle, SessionIo or ClientWorker teardown, multi-peer ownership, CPU or FD spin, or terminal-state versus live-runtime identity. Teardown-lens fields are not required.

This ticket is not a consumer of Hub session-type eligibility work.

## Relation to the 2026-08-16 plan draft

A prior Plan visit for this ticket committed a plan on a discarded branch (commit `2893cc8`, base `c72712e`). This plan keeps its structure and updates it for current main and for vault notes captured after that draft:

1. Main has moved to `674d2df`. `separators_close_when_item_bytes_fit_but_commas_do_not` and `near_limit_snapshot_assembly_stays_within_owner_turn` already use the `Duration::MAX` bounded-loop pattern on main.
2. Three vault notes captured after the draft change the design: [[empty-and-more snapshot pages close as oversized under elapsed cuts]], [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]], and [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]].
3. The draft kept the close-on-empty-page rule and tested elapsed only on the inner function. The vault evidence shows that rule is itself a production nondeterminism, not only a test coupling. This plan replaces the bare `more` flag with a typed page-cut classification. Section "Locked implementation" states the exact rule.

## Revision 2 (Plan Review findings)

Plan Review `review_1787088180_679716` requested changes. This revision resolves:

1. `finding_1787088180_455370` — the per-turn byte check now counts one real envelope, and the reported page bytes include it. Section "Locked implementation" states the exact arithmetic.
2. `finding_1787088180_475315` — the fresh-page capacity is now a parameter, separate from the remaining turn budget. Production passes `SESSION_DELIVERY_MAX_BYTES`; the separator test passes `DAEMON_MAX_FRAME_BYTES`, so its half-megabyte rows reach the cumulative comma boundary instead of closing as `OversizedRow`.
3. `finding_1787088180_996104` — the gate sequence now builds `botster-session-worker` (README requirement) before the single binding `./test.sh --locked` run. The reviewer's base run failed 8 tests solely from that missing binary; that is a prerequisite, not a product failure.
4. `finding_1787088180_988415` (process) — the next step-advance call carries the full required evidence fields. No duplicate artifact or checklist is created for this repair alone.

## Revision 3 (run restart, independent base re-verification)

Run `run_1787087218_390370` ended after committing Plan rev 2 (`af6a4520`) but before step advancement. Run `run_1787092386_910379` restarted the Plan step on a fresh worktree from the same base, `origin/main` at `674d2df`. Commit `af6a4520` is not an ancestor of this run's branch, so this revision recommits the plan here.

This revision changes no scope, design, or acceptance content. The Plan agent for this run independently re-verified every load-bearing code fact on this worktree before adoption:

- `take_snapshot_item_page` still clones `items` and encodes a stub-envelope trial frame per candidate (`src/daemon_entity_subscriptions.rs:1069-1078`).
- The empty-page `more` close rule is unchanged (`:1142-1143`); `encoded_item_bytes` still re-encodes accepted items (`:1145`, fn at `:1231`).
- Constants unchanged: `SESSION_DELIVERY_MAX_ITEMS = 16`, `SESSION_DELIVERY_MAX_BYTES = 64 KiB`, `SESSION_DELIVERY_MAX_ELAPSED = 8 ms`, `MAX_OWNER_TURN_MS = 25`, `DAEMON_MAX_FRAME_BYTES = 1 MiB`.
- Production callers unchanged: owner-loop delivery (`:612`) and `try_resync_from_projection` (`:1333`).
- Wall-clock primary asserts remain at `:2799`, `:2866`, `:3116`; assembly test call sites remain at `:2854`, `:2938`, `:2958`, `:3030`, `:3167`, `:3326`, `:3430`, `:3467`.
- README worker-build prerequisite remains (`README.md:42`); tracked `.gitignore` is 53 bytes and matches HEAD; the worktree path contains no `:`.

## Revision 4 (Plan Review finding_1787093543_426311)

Plan Review `review_1787093543_793312` (run `run_1787092386_910379`) found one product defect in the locked paging rule: the `OversizedRow` predicate checked `envelope + encoded_item_len > capacity` while `candidate_charge` also includes `separator_bytes(assembled_item_count + accepted_count, 1)`. After one or more assembled items, a candidate sized exactly to `envelope + item == capacity` passed the `OversizedRow` check but could never pass the `ByteBudget` check, so every full-capacity turn returned an empty `ByteBudget` cut — an undetected livelock that contradicts the no-livelock assumption and the ticket's by-construction requirement.

This revision:

1. Adds the positional separator term to the `OversizedRow` predicate, so the no-progress predicate charges exactly what acceptance would charge (Locked implementation, classification step 4).
2. Restates the no-livelock assumption with the invariant that makes it hold: any candidate surviving `OversizedRow` fits a fresh full-capacity page at its position.
3. Adds the later-page separator boundary acceptance test: assemble one item, place the next row at the exact no-comma capacity boundary, and prove deterministic closure instead of repeated empty `ByteBudget` cuts, with a one-byte-headroom control variant that is accepted.
4. Notes the consequence: oversized classification is position-dependent — a row admissible as the first item can close as oversized at a later position. This matches the one-frame snapshot contract; the cumulative frame-limit check already behaves this way at the `DAEMON_MAX_FRAME_BYTES` level.

The existing separator-overflow test keeps `DAEMON_MAX_FRAME_BYTES` capacity and remains reachable: each half-megabyte row fits the predicate individually, and the comma overflow is still caught by the cumulative frame-limit check.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role and overlay:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]

Targeted notes:

- [[Snapshot page accounting must charge incremental item bytes not a growing frame encode]]
- [[empty-and-more snapshot pages close as oversized under elapsed cuts]]
- [[A separator-boundary unit test flakes when MAX_OWNER_TURN_MS cuts the first half-megabyte page]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub background fairness must stay policy-neutral]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[a regression test must be shown to go red with the fix reverted]]

Process notes:

- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]
- [[cross repo dependency registration must use dependency repo target]]

Not loaded:

- [[project-pipelines-playbook]] — this ticket does not change Project Pipelines package or plugin paths.
- [[botster runtime teardown lenses]] — runtime-teardown class does not apply; see above.
- [[spa-patterns]] — no SPA surface is in scope; the affected file is Hub Rust daemon code only.

## Context loaded

Ticket contract:

- Make session snapshot page assembly honor item, encoded-byte, and elapsed budgets by construction.
- Use exact incremental JSON charge, including the fixed frame envelope and array separators.
- Do not use wall-clock assertions as the primary proof.
- Do not change owner-loop scheduling, PTY routing, WebRTC teardown, local runtime ownership, or event delivery priority.
- Required proof: focused unit tests for item, byte, separator, envelope, oversized-row, elapsed, and multi-page completion behavior; no clone-and-reserialize growth in the page loop; repository gates; one clean default-concurrency `./test.sh --locked` without retry.

Code facts on base `674d2df`, all in `src/daemon_entity_subscriptions.rs`:

1. `take_snapshot_item_page` (line 1050) clones the accepted `items` vector for every candidate and encodes a full trial `DaemonEntityFrame::Snapshot`. The cost is quadratic in accepted items per page.
2. The trial frame uses a stub envelope: empty `subscription_id`, `snapshot_seq: 0`, `resync_reason: None`. The real envelope from `snapshot_envelope_bytes` (line 1247) uses the real subscription id, `state.next_seq`, and `state.resync_reason`. The two charges disagree. The trial charge also omits the separator that joins this page to already-assembled items.
3. `continue_session_snapshot_assembly` (line 1103) classifies every empty page with `more == true` as `close_oversized_session_snapshot` (line 1142). `take_snapshot_item_page` returns that shape in three distinct situations that only one of which is oversized data:
   - the single candidate row cannot fit the byte budget;
   - `max_elapsed` expires before the first item (the owner loop passes `SESSION_DELIVERY_MAX_ELAPSED.saturating_sub(started.elapsed())`, which can be near zero at line 620);
   - the remaining per-turn byte budget is small (the owner loop passes `SESSION_DELIVERY_MAX_BYTES - delivered_bytes` at line 619, which can be as low as 1 byte after earlier subscribers).
   In the last two situations a healthy subscriber receives a spurious `entity_provider_frame_too_large` error and its snapshot assembly is destroyed. Whether that happens depends on scheduling and on subscriber position in the turn. That is the production nondeterminism this ticket removes.
4. `encoded_item_bytes` (line 1231) re-encodes every accepted item a second time after the page returns.
5. Two byte regimes exist and must stay separate:
   - Turn work budget: `SESSION_DELIVERY_MAX_ITEMS = 16`, `SESSION_DELIVERY_MAX_BYTES = 64 KiB`, `SESSION_DELIVERY_MAX_ELAPSED = 8 ms`. Exhaustion means yield and continue next turn.
   - Frame limit: `DAEMON_MAX_FRAME_BYTES = 1 MiB` on the one assembled Snapshot frame. Violation means close with `entity_provider_frame_too_large`.
6. All assembled pages accumulate into one `DaemonEntityFrame::Snapshot` that is sent once when assembly completes. Pages are work slices, not separate frames.
7. Production callers: `SubscribeEntities` registration (line 1333) and owner-loop subscriber delivery while `DeliveryPhase::Assembling` (line 612). Both call `continue_session_snapshot_assembly`.
8. The `Full` requeue path (line 1199) puts assembled items back and retries; `assembled_item_bytes` stays charged. Any accounting change must keep that path consistent.
9. Existing tests on main: `separators_close_when_item_bytes_fit_but_commas_do_not` (line 3218) and `near_limit_snapshot_assembly_stays_within_owner_turn` (line 3122) already use `Duration::MAX`. Wall-clock `started.elapsed() < MAX_OWNER_TURN_MS` primary asserts remain at lines 2799 (`paged_delivery_stays_within_owner_turn_for_a_large_registry`), 2866 (`first_session_snapshot_is_complete_and_assembled_in_pages`), and 3116 (`no_removal_scan_stays_within_owner_turn`). `oversized_first_snapshot_closes_the_subscription` (line 2990) calls assembly with production `SESSION_DELIVERY_MAX_ELAPSED`.
10. Budget constants stay product contracts: `SESSION_DELIVERY_*`, `MAX_OWNER_TURN_MS = 25`, `MAX_READY_OPERATION_WAIT_MS = 50`, `DAEMON_MAX_FRAME_BYTES`.

Process facts:

- Repo placement: `docs/plans/` is living Hub plan prior art; this plan belongs there. `docs/reports/` receives the Implement report.
- Worktree hygiene: tracked `.gitignore` matches HEAD (53 bytes, non-empty). The worktree path has no `:`. A `CARGO_TARGET_DIR` override is not required.
- Gates: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --doc --workspace`; one default-concurrency `./test.sh --locked`.
- README requires `cargo build --locked -p botster-core-daemon --bin botster-session-worker` once per checkout before the suite. Plan Review's independent base run from a clean worktree confirmed the cost of skipping it: 8 test failures, all rooted in the missing `target/debug/botster-session-worker` binary, with no other base failure root.
- One Plan vault checklist already exists for this ticket (`checklist_1786912958_983411`). This visit reuses it and creates no duplicate.

## Scope

Stay on `botster-hub` `tgt_7e208a0c76a44980a83b63af976b1f22`. Change only session snapshot page assembly and its unit proofs in `src/daemon_entity_subscriptions.rs`.

1. Replace the clone-and-reserialize loop in `take_snapshot_item_page` with exact incremental charging. Encode each candidate item once. Never clone the accepted items. Never encode a trial frame.
2. Charge with the real envelope. `continue_session_snapshot_assembly` computes `snapshot_envelope_bytes(subscription_id, state)` once per call and passes it in. Delete the stub-envelope trial.
3. Replace the bare `more: bool` page result with a typed page-cut classification: `Complete`, `ItemBudget`, `ByteBudget`, `Elapsed`, `OversizedRow`. Only `OversizedRow` and the cumulative frame-limit check may close the subscription as oversized. `ItemBudget`, `ByteBudget`, and `Elapsed` cuts — including empty ones — set `needs_delivery` and return `Continue { more: true }`.
4. Define `OversizedRow` deterministically: the candidate cannot fit even as the sole item of a fresh page. The rule depends only on the encoded row, the envelope, and the fresh-page capacity parameter, which callers set from constants — never on the remaining turn budget or elapsed time. Section "Locked implementation" gives the exact predicate.
5. Return the exact charged byte totals from the page function, with the envelope counted in every page call's turn-budget check and reported bytes. Delete the second `encoded_item_bytes` re-encode pass in assembly.
6. Keep one accounting source. The page charge, the cumulative `assembled_item_bytes`, and the frame-limit check must use the same item, separator, and envelope arithmetic, so that on completion the charged total equals the encoded length of the sent frame exactly.
7. Convert the wall-clock primary asserts at lines 2799, 2866, and 3116 to deterministic work-bound asserts (per-call `page.items` and `page.bytes` against the passed budgets). Keep their behavior coverage. Do not change production budget constants.
8. Move `oversized_first_snapshot_closes_the_subscription` from production `SESSION_DELIVERY_MAX_ELAPSED` to `Duration::MAX`, because after item 3 an elapsed cut returns `Continue` and the test would otherwise flake in the opposite direction.
9. Add the focused construction tests listed under acceptance.
10. Audit every test call site of `continue_session_snapshot_assembly` (lines 2854, 2938, 2958, 3030, 3167, 3326, 3430, 3467) for expectations that depended on the old empty-page close rule or on the stub envelope; update them to the typed contract.
11. If default-concurrency `./test.sh --locked` fails on another root, create a separate blocker ticket. Do not fold that root into this change.

Every changed line must serve the ticket, a required convention, or cleanup made necessary by this change.

## Non-scope

- Owner-loop scheduling, Maintenance versus Pump fairness, `should_start_background_work`, or anything in `src/daemon_maintenance.rs` (that is `ticket_1786912569_840742`). The outer delivery loop at lines 593-660, including its budget-splitting arithmetic, stays unchanged.
- `deliver_projection_delta_page` and delta delivery. Deltas encode one frame per item and do not have the quadratic shape.
- PTY process lifecycle fixtures (`ticket_1786912572_610381`), local runtime ownership, session-worker routing.
- WebRTC teardown, peer close, adapter close.
- Event-delivery priority, host-control lanes, package-event routing.
- Core pin or Core page API changes; hub-client DTOs; generated TypeScript; hub-test-support; Web, TUI, Workspaces, or Project Pipelines code. `DaemonEntityFrame` wire shape does not change.
- Raising any published budget constant.
- Serial-only acceptance (`--test-threads=1` or a suite-wide mutex).
- Cherry-pick of superseded `ticket_1786875812_242946`.
- Runtime-teardown class work.

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Session snapshot page assembly and session-entity subscribe delivery | `botster-hub` | Change and prove here. |
| Sliced Core lifecycle observe and baseline pages | `botster-core` | Consume unchanged. |
| External client DTOs | `botster-hub-client` | No contract change. `DaemonEntityFrame` bytes on the wire are identical. |
| Terminal bytes and attach snapshots | Core SessionIo / ClientWorker | Do not touch. |
| Owner-loop background scheduling | `botster-hub`, `ticket_1786912569_840742` | Out of scope here. |

No cross-repository dependency is registered. The one registered dependency (`ticket_1786913892_208903`) is closed and its fix `6d69028` is on the base. If Implement finds a missing Core seam, stop and register a blocking ticket against the `botster-core` spawn target (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) instead of broadening this run.

## Assumptions and unknowns

### Assumptions

- The production defects are exactly the four numbered code facts above; all are in `take_snapshot_item_page` and `continue_session_snapshot_assembly` on current main.
- `state.next_seq` and `state.resync_reason` do not change during one assembly pass, so one envelope charge per assembly call is exact. `reset_snapshot_assembly` starts a new pass when the source sequence moves.
- Reclassifying empty `ByteBudget`/`Elapsed` cuts as `Continue` cannot livelock: each new owner turn starts with the full `SESSION_DELIVERY_*` budgets, and the `OversizedRow` predicate charges the candidate's full positional cost (`envelope + item + positional separator`) against the full capacity. Therefore any candidate that survives the `OversizedRow` check fits a fresh full-capacity page at its position, and a full-budget turn accepts it. An empty `ByteBudget` cut can only occur when the remaining budget is below capacity, which a later turn restores. (Revision 4: this argument requires the separator term inside the `OversizedRow` predicate; see finding_1787093543_426311.)
- Closing rows above the fresh-page threshold (the current behavior for genuinely oversized rows) remains the correct product policy; this plan makes the closure deterministic, it does not remove it.
- `serde_json` encoding of a fixed `Value` is deterministic, so exact byte equality between the charged total and the sent frame is a valid oracle.

### Unknowns Implement must resolve with evidence

- Exact encoded sizes for item, envelope, and separator fixtures. Measure them with `serde_json` on the same values the page loop encodes; do not hard-code guessed lengths.
- Whether any audited test call site depends on the old spurious-close behavior in a way that reveals a real product expectation. If a client-visible contract genuinely relied on the close, stop and ask a human before weakening it.
- Whether `./test.sh --locked` exposes a different failure root. If it does, stop and open a blocker; do not retry for a chance green.

## Affected surfaces/files

Expected:

- `src/daemon_entity_subscriptions.rs` — rewritten `take_snapshot_item_page` with typed cuts and exact incremental charge; envelope pass-through and reclassified close rule in `continue_session_snapshot_assembly`; removed double re-encode; new and converted unit tests.
- `docs/plans/make-session-snapshot-paging-deterministic.md` — this plan.
- `docs/reports/` — Implement report.

Verification-only, no expected change:

- `src/daemon_transport.rs` (module parent, `DAEMON_MAX_FRAME_BYTES`), `src/daemon_maintenance.rs`, `test.sh`, CI workflow.

## Locked implementation

Keep the public delivery contract. Change how a page accepts a candidate and how the caller classifies the result.

Two exact byte quantities exist and must never be conflated:

- Per-turn encoded work for one page call: `envelope + accepted item bytes + separator bytes`. This is checked against the remaining turn byte budget and is reported as the page's byte total (`DeliveryPage.bytes`).
- Cumulative frame bytes for the one assembled Snapshot frame: `assembled_item_bytes + envelope`, where `assembled_item_bytes` accumulates only item and separator bytes across pages. This is checked against `DAEMON_MAX_FRAME_BYTES`.

The envelope therefore counts once in every page call's turn-budget check and once in the final frame; it is never added to `assembled_item_bytes`.

Page charge for one candidate:

```
candidate_charge = encoded_item_len
                 + separator_bytes(assembled_item_count + accepted_count, 1)
```

where `separator_bytes` follows the existing `snapshot_separator_bytes` semantics (one comma per join).

`take_snapshot_item_page` must:

1. Accept from the caller: the assembled item count, the assembled item bytes, the exact envelope bytes, `max_items`, the remaining turn byte budget, the full fresh-page byte capacity, and `max_elapsed`. The capacity is the byte budget a fresh call receives; production callers pass `SESSION_DELIVERY_MAX_BYTES`, and tests may pass a larger value such as `DAEMON_MAX_FRAME_BYTES`.
2. Walk `rows_after` in the existing id order.
3. Encode each candidate item once with `serde_json::to_vec`. Keep the `Value` for the page; keep the length for the charge. No `items.clone()`. No trial `DaemonEntityFrame::Snapshot` encode.
4. Classify, in this order, per candidate:
   - `ItemBudget` cut when accepted items reach `max_items`.
   - `Elapsed` cut when `started.elapsed() >= max_elapsed`.
   - `OversizedRow` when the candidate cannot fit even a fresh full-capacity page at its actual frame position: `envelope + encoded_item_len + separator_bytes(assembled_item_count + accepted_count, 1) > capacity`. The separator term is the same one `candidate_charge` uses, so the no-progress predicate charges exactly what acceptance would charge. This predicate uses only the row, the envelope, the candidate's deterministic frame position, and the capacity parameter — never the remaining budget or elapsed time. Callers pass constants, so closure stays a function of data and deterministic assembly state, not scheduling. (Revision 4: the separator term was missing; without it a later-positioned row at the exact `envelope + item == capacity` boundary passes the OversizedRow check but can never pass the ByteBudget check, yielding an empty `ByteBudget` cut every turn forever.)
   - `ByteBudget` cut when `envelope + page_charge + candidate_charge` exceeds the remaining turn byte budget. The cumulative frame-limit check stays at the assembly level — the page loop only enforces the turn budget.
   - Otherwise accept: push the value, add `candidate_charge` to `page_charge`, record `last_id`.
5. Return the accepted values, the reported page byte total `envelope + page_charge`, the item-and-separator charge `page_charge`, `last_id`, and the typed cut.

`continue_session_snapshot_assembly` must:

1. Compute `snapshot_envelope_bytes(subscription_id, state)` once and pass it to the page function, together with `SESSION_DELIVERY_MAX_BYTES` as the capacity on the production path.
2. Close with `close_oversized_session_snapshot` only when the page reports `OversizedRow`, or when the cumulative check `assembled_item_bytes + page_charge + envelope > DAEMON_MAX_FRAME_BYTES` fails using the same arithmetic the page used.
3. For `ItemBudget`, `ByteBudget`, and `Elapsed` cuts — with or without accepted items — store progress, set `needs_delivery = true`, and return `Continue { more: true }`.
4. Report `DeliveryPage.bytes = envelope + page_charge` to the caller, and add only `page_charge` to `assembled_item_bytes`. Do not re-encode accepted items.
5. Keep the `Full` requeue path consistent: requeued items keep their charged `assembled_item_bytes`.

The outer delivery loop keeps accumulating `delivered_bytes` from `DeliveryPage.bytes` unchanged. Because each assembling page now reports its envelope, a multi-page assembly reports the envelope once per turn it runs in; this over-reports total work slightly in the conservative direction and keeps every single turn's admitted encode work at or under `SESSION_DELIVERY_MAX_BYTES`.

Exactness invariant: when assembly completes and sends the frame, the accumulated charge (`assembled_item_bytes + envelope`) must equal `serde_json::to_vec(&sent_frame).len()`. A unit test asserts this equality.

Source scan after the change: `take_snapshot_item_page` must not contain `items.clone()` or a per-candidate `DaemonEntityFrame::Snapshot` encode.

The exact enum and struct names are the implementer's choice; the classification rules, the two byte quantities, and the capacity parameter above are not.

## Risks

- Behavior change: subscribers that previously received a spurious oversized close under load now retry and complete. This is the intended fix, but audited tests (Scope item 10) may encode the old behavior. Mitigation: audit every assembly call site; treat a genuinely load-independent expectation as a stop-and-ask signal.
- Behavior change: the real envelope is larger than the stub (real subscription id, real `next_seq`, possible `resync_reason`). A snapshot near the frame limit that previously passed the under-charging stub check now closes as oversized. This is a correctness fix; record it in the Implement report.
- The two byte regimes can be conflated during implementation. Mitigation: the typed cut plus the exactness invariant test; `ByteBudget` never closes, `OversizedRow` and the cumulative frame check always close; the envelope counts in every turn-budget check but only once in the frame.
- `DeliveryPage.bytes` now includes the envelope for assembling pages, so the outer loop's `delivered_bytes` counts one envelope per assembling subscriber per turn. The effect is conservative: each turn admits at most `SESSION_DELIVERY_MAX_BYTES` of encoded work, never more. No mitigation is needed beyond asserting work bounds against the new definition.
- A test-only cheaper encoder would leave the production path quadratic. Mitigation: the tests drive `continue_session_snapshot_assembly` / `take_snapshot_item_page`, the functions `SubscribeEntities` and owner-loop delivery call.
- `--exact` on an unqualified lib filter can exit 0 after zero tests. Mitigation: use fully qualified test paths and require output showing exactly one test ran.
- Default-concurrency `./test.sh --locked` can fail on an unrelated root, including the remaining wall-clock asserts in `src/daemon_maintenance.rs` (lines 2152, 2217), which belong to the scheduler ticket. Mitigation: one clean run without retry; a different root becomes a separate blocker.

## Implementation sequence

1. Confirm the worktree base is `674d2df`, tracked `.gitignore` is non-empty, and the path has no `:`.
2. Confirm `take_snapshot_item_page` on the worktree still clones and re-encodes the growing trial frame.
3. Introduce the typed page-cut result and rewrite `take_snapshot_item_page` with exact incremental charging.
4. Pass the real envelope from `continue_session_snapshot_assembly`; reclassify the close rule; remove the double re-encode.
5. Add the construction tests; convert the three wall-clock primary asserts; move the oversized test to `Duration::MAX`; audit the remaining assembly call sites.
6. Red proof: ablate the byte charge (accept every candidate) and show the byte-budget test fails; separately restore the old empty-page close classification and show the elapsed reclassification test fails. Restore before commit.
7. Build the README-required worker binary (`cargo build --locked -p botster-core-daemon --bin botster-session-worker`), then run the gates on this worktree: fmt, clippy `--locked`, doc tests, focused `--exact` tests, then one default-concurrency `./test.sh --locked` without retry.
8. Write the Implement report under `docs/reports/`.

## Acceptance checks/tests

All focused tests drive the production functions in `src/daemon_entity_subscriptions.rs`. Elapsed control uses `Duration::MAX` (never cut) or `Duration::ZERO` (always cut). No wall-clock assert is a primary proof anywhere in the changed tests.

| Behavior | Proof |
| --- | --- |
| Item budget | `max_items = 1` over a two-row projection returns one item and an `ItemBudget` cut; a later call resumes after `last_id` and completes. |
| Byte budget | With measured `envelope` and first-candidate charge `c1`: a remaining budget of `envelope + c1` accepts one item and cuts `ByteBudget` before the second; a remaining budget of `envelope + c1 - 1` accepts zero items and returns `Continue { more: true }`, not a close; a remaining budget of `c1` alone (no envelope headroom) also accepts zero items. The last case is the envelope-admission boundary: envelope inclusion is what flips the decision. |
| Separator | `separators_close_when_item_bytes_fit_but_commas_do_not` stays green against the rewrite. The test passes `DAEMON_MAX_FRAME_BYTES` as both the remaining budget and the fresh-page capacity, so its half-megabyte rows are admitted per page and never classify as `OversizedRow`; the comma overflow is caught by the cumulative frame-limit check and closes oversized. |
| Envelope | The charge uses the real envelope: with a nonzero `next_seq` and a set `resync_reason`, a page that fits under the stub envelope but not the real one is cut/closed correctly. Assert the envelope value equals `snapshot_envelope_bytes` for the same state. |
| Oversized row | A row with `envelope + item + positional separator > capacity` closes with one `entity_provider_frame_too_large` error frame and today's state transitions; the same row is accepted when the test passes a larger capacity, proving the predicate uses the capacity parameter. Production capacity is `SESSION_DELIVERY_MAX_BYTES`. `oversized_first_snapshot_closes_the_subscription` converts to `Duration::MAX`. |
| Later-page separator boundary (Revision 4) | Assemble at least one item first. Size the next row so `envelope + item == capacity` exactly (no comma headroom): the next full-capacity call classifies `OversizedRow` and closes deterministically with one error frame — it does not return repeated empty `ByteBudget` cuts. A control variant with one byte of comma headroom (`envelope + item + 1 <= capacity`) is accepted at full capacity, proving the boundary sits exactly on the separator term. |
| Elapsed | With `Duration::ZERO` and normal rows, `take_snapshot_item_page` returns an empty `Elapsed` cut, and `continue_session_snapshot_assembly` returns `Continue { more: true }` with `needs_delivery` set and sends no error frame. A later call with `Duration::MAX` completes the snapshot. This is the regression test for the spurious-close defect. |
| Multi-page completion | A registry assembles across multiple calls at one source sequence; exactly one Snapshot frame is delivered; items are complete and in id order; the exactness invariant holds: charged total equals the encoded sent frame length. |
| No clone-and-reserialize | Source scan: no `items.clone()`, no per-candidate frame encode. Ablation red proof per sequence step 6. |
| Wall-clock conversion | Lines 2799, 2866, 3116 assert per-call `page.items`/`page.bytes` work bounds instead of `started.elapsed()`. Production constants unchanged. |

Required Implement commands on this Hub worktree:

1. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` — README-required once per checkout; without it the suite fails Spawn tests on the missing `target/debug/botster-session-worker`. This is a prerequisite, not a product failure, and it must complete before the binding suite run.
2. `cargo fmt --all -- --check`
3. `cargo clippy --workspace --all-targets --locked -- -D warnings`
4. `cargo test --doc --workspace`
5. Focused `--exact` runs of each new/converted test by fully qualified path, each reporting exactly one test run.
6. Binding: one `./test.sh --locked` at Cargo default concurrency, after prerequisite 1 is ready. Exit 0. No `--test-threads=1`. No retry.
7. Red-when-reverted evidence for the byte-budget and elapsed reclassification tests, restored before commit.

Production-path proof:

- `DaemonRequest::SubscribeEntities` still registers through `continue_session_snapshot_assembly` → `take_snapshot_item_page` (line 1333 call path).
- Owner-loop session delivery still calls the same assembly function while `DeliveryPhase::Assembling` (line 612 call path).
- The focused tests execute those production functions; code existence alone is not proof.

Downstream: no Web, TUI, hub-client, or hub-test-support change is required; the wire shape of `DaemonEntityFrame` is unchanged.

## Product decision ledger

- Default: exact incremental charge with typed page cuts; only data-dependent conditions close a subscription; scheduling-dependent conditions always yield.
- Non-goals: owner-loop rewrite, PTY fixtures, WebRTC teardown, budget inflation, Core changes, DTO changes.
- Follow-up-ok: separate blocker for any unrelated `./test.sh --locked` root; vault capture after Implement lands.
- Ask-human if: an audited test reveals a client contract that relies on the old empty-page close; or making paging tests pass would require owner-loop scheduling changes; or a Core seam is missing.

## Vault gaps worth capturing

Capture after Implement confirms the change; do not write speculative notes during Plan.

1. The empty-page oversized close is a production defect, not only a test coupling: small remaining turn budgets and elapsed cuts can close healthy subscribers. This extends [[empty-and-more snapshot pages close as oversized under elapsed cuts]], which currently frames the shape as a test-hygiene rule.
2. The typed page-cut contract: turn-budget exhaustion yields, only data-versus-constant violations close. Deterministic closure is a function of data and constants, never of scheduling.
3. The stub-envelope under-charge (`subscription_id: ""`, `snapshot_seq: 0`) as a general gotcha: budget trials must charge the identity that will actually be sent.
