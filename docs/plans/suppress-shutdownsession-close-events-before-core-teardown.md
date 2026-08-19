# Plan: suppress ShutdownSession close events before Core teardown

Ticket: `ticket_1787143511_231816`
Run: `run_1787143511_194671`
Target: `tgt_7e208a0c76a44980a83b63af976b1f22` (botster-hub)
Base: `origin/main` at `0a3458a`

## Target repository and routing

- Target repository: **botster-hub**. The ticket names the target repository and target_id explicitly.
- Repository playbook loaded: [[botster-hub-playbook]].
- Role playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]].
- Class overlay loaded: [[botster runtime teardown lenses]] (this ticket is runtime-teardown class).
- Targeted atomic notes loaded:
  - [[host ShutdownSession classification must call the exact-session Core query]]
  - [[Unix mux host events are unsolicited control frames]]
  - [[host reconciliation must not rewrite a completed Core adapter close reason]]
  - [[WebRTC host events use unsolicited daemon-event delivery]] (via charter list; delivery contract only)
  - [[Core terminal subscription ownership is session, subscription, and generation]] (identity rule)
  - [[pre READY attach failure creates no attach ownership]] (late-attach admission)
- [[project-pipelines-playbook]] not loaded: no Project Pipelines package/plugin path is in scope.

## Context loaded from the repository

- `src/daemon_transport.rs:3480-3522` — the `ShutdownSession` handler. Classification (`classify_shutdown_session`, line 4738) uses the exact-session Core query `observe_session_lifecycle`. On the `Active | Stopping | Err(_)` fall-through the handler sends the Core `Shutdown` request first and calls `suppress_unix_session_close_events` / `suppress_webrtc_session_close_events` (lines 4673-4687) **after** the Core call, on both the success and error branches. This is the defect window.
- `src/daemon_transport.rs:5049-5131` — `run_close_events_phase`. The pump sweep calls `queue_closed_subscription_events_bounded` per admitted mux and classifies each closed, unreported route with `session_close_event_decision_for`: registry `Running` → emit, other found state → mark reported silently, `Absent`/`Err` → retry later.
- `src/unix_terminal_adapter.rs:282-522` and the mirrored `src/webrtc_terminal_adapter.rs` — per-connection mux with `routes: BTreeMap<(session, subscription, generation), Route>`, `suppress_sessions: BTreeSet<String>`, `suppress_generations: BTreeSet<(String, String, u64)>`. Both suppression sets are insert-only for the connection lifetime. A suppressed route is marked `reported` without an event. `Detach` (daemon_transport.rs:3400, 3411) already uses exact-key `suppress_generation`.
- `src/daemon_attach_stream.rs:354-431` — `close_adapters_for_session` performs host-side closes; `reconcile_inventory_slice` closes routes whose Core generation is stale or absent.
- Parent-run prototype: commit `6aac388` moved the two session-wide suppress calls before the Core request; commit `b7fb615` reverted it to keep the parent ticket test-only and registered this ticket as the owner of the production change. Neither commit is on `main`; this plan builds on `main`.
- CI gates (`.github/workflows/ci.yml`): `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `./test.sh --locked`.

## Problem statement

During an Active `ShutdownSession`, Core teardown closes the session's terminal adapters (`core` close, `host_closed == false`) while the registry row can still read `Running`. Because suppression is installed only after the Core `Shutdown` request returns, a CloseEvents pump pass in that window emits `TerminalSubscriptionClosed { reason: core_adapter_closed }`. That violates the host contract: process exit and `ShutdownSession` stay on lifecycle paths and must not surface as subscription close events. Review findings `finding_1787143211_492928` and `finding_1787143211_159905` require this production fix.

## Scope

1. In the `ShutdownSession` handler, install close-event suppression **before** the Core `Shutdown` request on the `Active | Stopping | Err(classify)` fall-through path, and remove the two post-request suppression call sites (success branch and error branch).
2. Change the suppression identity from session-wide to **exact route keys**. Add one method to each mux (`UnixConnectionMux`, `WebrtcConnectionMux`), for example `suppress_session_route_generations(&self, session_id)`: snapshot the mux's current routes for that session id and insert each exact `(session_id, subscription_id, generation)` key into the existing `suppress_generations` set. Update `suppress_unix_session_close_events` / `suppress_webrtc_session_close_events` to call it across all admitted connections.
3. Remove the now-unused session-wide machinery (`suppress_sessions` set, `suppress_session` method, `session_is_suppressed` check) from both muxes, and update the two mux unit tests that exercise it (`close_event_slice_uses_keyed_suppression_without_cloning_the_prefix` in each adapter). This removal is forced by the change: the ticket requires that suppression never outlive the exact generations being torn down.
4. Tests and proofs listed under Acceptance checks.

Why exact keys and not the parent's session-wide prototype: the per-mux suppression sets are insert-only for the connection lifetime. A session-wide entry would permanently suppress every future route for a reused session id on that connection, which directly violates the ticket's replacement-owner requirement. Exact keys reuse the shipped `Detach` suppression mechanism unchanged.

## Non-scope

- The parent PTY-oracle test ticket (`ticket_1786912572_610381`) and its fixtures; that run stays test-only and touches `tests/hub_daemon_lifecycle/session_fixtures.rs` plus docs, so there is no file overlap.
- Changing the `Cleanup` classification policy (adapters stay open; comment at daemon_transport.rs:3484 stands).
- Changing the `Missing` path policy (host-close adapters plus `unknown_session` cleanup, no suppression).
- Changing the error-path host-close policy (`close_adapters_for_session` at line 3508 stays; see teardown bounds below).
- Mux route retirement / unbounded route-map growth (pre-existing; recorded as a vault gap).
- Any client DTO or protocol change: the fix changes only whether an already-optional event is emitted.

## Ownership boundaries and cross-repo dependencies

- Hub owns host close-event policy, suppression, and the ShutdownSession control path (this change).
- Core owns subscription lifecycle, the hard-stop synchronous adapter close, and generation identity; the required Core APIs (`observe_session_lifecycle`, `terminal_subscription_generation`, generation-bearing attach bind) are already shipped on `main`. **No cross-repository dependency is required.**
- The data plane (terminal bytes) and botster-hub-client DTOs are untouched.

## Runtime-teardown lens answers

- `teardown_class_applies`: yes. The ticket changes SessionIo/adapter close-event behavior on the session teardown path and covers terminal-state vs live-runtime divergence.
- `teardown_isolation`: the ownership set that dies is the target session's mux routes — the exact `(session, subscription, generation)` keys present at suppression time, across all admitted Unix and WebRTC connections. Sibling sessions on the same connection and other connections keep their routes, their close events, and their suppression state. One failed teardown cannot silence a sibling.
- `teardown_bounds`: suppression is two bounded mutex inserts per route on the control loop; no waits, no new blocking. The Core `Shutdown` request keeps its existing single-request deadline (commit `d21c440`). On Core error the handler keeps the shipped bounded recovery: host-close the session's adapters, re-classify once, return a typed result. Nothing in this change can hang the control plane. The suppression sets grow monotonically per connection, matching the shipped `Detach` behavior; growth is bounded by shutdown/detach volume over one connection lifetime (recorded as a vault gap, not changed here).
- `late_message_matrix`:

| Message | Creates durable ownership | Tag | Rejection after teardown | Residual sweep if racing |
|---|---|---|---|---|
| Attach | mux route + stream + Core subscription | Core-issued `(session, subscription, generation)` at bind | Core rejects attach to a missing/stopping session; [[pre READY attach failure creates no attach ownership]] | control-loop serialization means no route can register between suppression and the Core call; `reconcile_inventory_slice` closes stale routes afterward |
| Detach | removes ownership; inserts its own exact suppress key | exact key | typed UnknownSession error | shipped behavior, unchanged |
| Drain | none (reads route-owned queues) | n/a | `unknown_session` OperatorError | n/a |
| SendInput / ModeGatedInput / Resize | none | n/a | typed runtime error | n/a |
| Second/concurrent ShutdownSession | none; inserts idempotent suppress keys | exact keys | classify returns Missing/Cleanup typed responses | set inserts are idempotent |
| Spawn with reused session id | new session, new subscriptions, later generations | new generation differs from every suppressed key | n/a | replacement owner unaffected (proof required) |

- `production_path_proof`: exact path — daemon request `ShutdownSession` → `handle_runtime_control_request` → `classify_shutdown_session` (exact-session Core query) → fall-through → **suppress exact route keys on every admitted mux** → Core `Shutdown` request → Core hard-stop closes adapters on the host tick → later CloseEvents pump passes observe closed, suppressed routes and mark them reported with no event. Live oracles: keep-reading Unix and WebRTC observers through the real daemon assert the typed shutdown response and the absence of `TerminalSubscriptionClosed`, while lifecycle progress (ProcessExited / registry transition) is proven through the production observe path. Red-on-revert: see Acceptance check 6.
- `ownership_identity`: suppression keys are the Core-issued exact `(session_id, subscription_id, generation)` triples captured from live mux routes. A replacement owner for a reused session id gets a later generation and never matches a suppressed key. Both queue orders hold: keys inserted before the close (`closed first` impossible for the dying generation) and a late close long after suppression (`message first`) both resolve to reported-without-event for the dying generation only. Assumption to verify during Implement: Core never reissues the same generation for the same `(session, subscription)` pair after session re-creation — the replacement-owner proof must use real Core-issued generations, not synthetic numbers.
- `sibling_fail_closed_policy`: on successful shutdown, sibling sessions and other connections are untouched (tested). On ultimate Core-shutdown failure with classification still Active, the requester receives the typed OperatorError, the target session's adapters are host-closed under suppression (shipped policy, ordering unchanged), and siblings keep working; no sibling is sacrificed and no wait is unbounded. Defined answer to the ticket's error-path question: **adapters do close under suppression on the Core-error path, and no close event is emitted**; the typed result on the request path is the client's signal. This matches shipped policy; only the suppression ordering changes.

## Assumptions and unknowns

- Assumption: route registration (attach) and ShutdownSession are serialized on the one daemon control loop, so a route snapshot at suppression time is complete. Implement must confirm no attach path registers mux routes off the control loop.
- Assumption: Core generations for a `(session, subscription)` pair are never reused across session re-creation. Verified by the replacement-owner proof with real Core-issued generations.
- Unknown: whether a deterministic in-handler red-on-revert oracle is achievable without a test seam. Fallback is the repo's accepted ablation-report form (`docs/reports/held-live-red-on-revert-ablation.txt` prior art); see Acceptance check 6.
- Unknown (pre-existing, out of scope): mux routes for `Absent` sessions are classified `None` forever and are never retired, leaving bounded-per-pass but repeated scan work. Recorded as a vault gap.

## Affected surfaces/files

- `src/daemon_transport.rs` — `ShutdownSession` arm reorder (lines 3480-3522); keyed suppression helpers (lines 4673-4687); in-file unit tests near line 8272.
- `src/unix_terminal_adapter.rs` — add exact-key session suppression method; remove `suppress_sessions` machinery; update mux unit tests.
- `src/webrtc_terminal_adapter.rs` — same, mirrored.
- `tests/hub_daemon_lifecycle/sessions.rs`, `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`, `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` — live proofs (extend; `webrtc_terminal_adapter.rs:1031` already asserts shutdown emits no close event and must stay green).
- `docs/reports/` — red-on-revert ablation report if the seam-based oracle is not feasible.

## Risks

- A deterministic revert oracle for a pure reordering is the hardest piece; the live race is timing-dependent. Mitigation: unit-level order-simulation oracles at the mux/handler seam plus the documented ablation form.
- Removing `suppress_sessions` touches both mux test files; keep the diff mechanical and separate from the ordering change in review terms.
- If Core can reissue generations for reused ids, exact keys could collide with a replacement owner; the identity proof with real generations guards this.
- Error-path host-close silently ends other clients' live subscriptions for a still-Active session (pre-existing shipped policy, now explicitly documented); not changed here.

## Acceptance checks/tests

1. Gates: `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; `./test.sh --locked`. Colon-free worktree confirmed; tracked `.gitignore` intact at HEAD.
2. Unit, both muxes: exact-key suppression marks a core-closed route reported without an event while the registry classifier answers `Running`; a later route for the same session and subscription with a higher generation still emits.
3. Unit, handler decomposition: Active classification with Core `Shutdown` error — suppression keys installed before the Core call, `close_adapters_for_session` host-close under suppression produces no event on a subsequent `Running` sweep, and `recover_from_exact_classify` returns the exact typed results per classification (Cleanup/Missing/Stopping/Active+already-gone/Active+error).
4. Live Unix success path: spawn + attach through the real daemon, `ShutdownSession`, keep reading; assert the typed response, lifecycle progress through the production observe path, and zero `TerminalSubscriptionClosed` frames.
5. Live WebRTC production path: the same absence proof over a negotiated WebRTC connection (protocol 7 close-event negotiation); keep `webrtc_terminal_adapter.rs:1031` green and extend it if it does not drive the Active classification.
6. Red-on-revert: prefer a deterministic seam oracle that observes suppression state at adapter-close time inside the Core call; if not feasible without production-code distortion, locally revert the ordering edit and record which live/unit oracle goes red in a `docs/reports/` ablation report (repo prior art).
7. Stopping path: `ShutdownSession` against a Stopping session returns the typed response and emits no close event.
8. Missing path: `ShutdownSession` for an unknown id returns `unknown_session` cleanup, emits no close event, and installs **no** suppression key (assert a later attach + close for that id behaves normally).
9. Late close work: run CloseEvents and inventory-reconcile passes after shutdown completes; suppressed keys stay silent and the close reason of already-closed adapters is not rewritten ([[host reconciliation must not rewrite a completed Core adapter close reason]]).
10. Replacement owner: reuse the session id after shutdown, attach, capture the real Core generation, and prove the new generation's close events flow while the old keys stay suppressed — exact session, subscription, and generation identity.
11. Sibling isolation: a second session's subscription on the same connection keeps streaming and keeps its own close-event behavior across the first session's shutdown.

## Vault gaps worth capturing

- Mux routes marked reported are never retired until connection death; interaction with `PUMP_MAX_ROUTE_ENTRIES_VISITED` scan budgets deserves a note.
- `Absent` classification returns `None` forever for orphan routes (permanent revisit work) — candidate gotcha note.
- After Implement: update the shutdown-classification note neighborhood with "ShutdownSession suppresses exact route generations before Core teardown", and mark [[host ShutdownSession classification must call the exact-session Core query]] as shipped behavior (its "not shipped yet" line is stale — classification already ships on main).
