# Stop failed local WebRTC peers from spinning botster-hub CPU

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`) |
| Pipeline worktree | this session worktree (branch `project-pipelines/ticket_1786327694_445993`) |
| Ticket | `ticket_1786327694_445993` |
| Run | `run_1786327694_835389` |
| Plan step | `bsg_plan` sequence 7 (`run_step_1786329069_778212`) after Plan Review sequence 6 `changes_required` |
| Base ref | `main` @ HEAD `1c6a41e36f6f1bcc09d52feb4f61ae507b9bc04f` |
| Plan revision | sequence 7 — feasible close-completion oracle (no `connection_state`) |
| Consumed webrtc | `0.20.0-rc.1` (`PeerConnection`: `close()`, `get_stats()` — **no** `connection_state()`) |

Resolved via `list_spawn_targets` against the ticket/run `target_id`. Do **not** infer ownership from the ambient process cwd.

### Parallel-ticket note (human-answered)

Plan Review asked about duplicate ticket `ticket_1786324642_480494` / run `run_1786324716_877362`. Human answer (`question_1786328159_412561`): **both tickets continue independently; outcomes will be compared.** Do not cancel or fold this run.

### Plan Review findings addressed in this revision

| Finding | Severity | Status in this plan |
| --- | --- | --- |
| `finding_1786328360_770815` peer_failed production-path proof | high | Addressed by locked **H1** |
| `finding_1786328360_792030` runtime drop / reuse | high | Resolved (H2/H3 + immediate drop) |
| `finding_1786328361_108325` vault checklist incomplete | medium | Resolved |
| `finding_1786328761_509663` entity-delivery + close proof | high | Entity-delivery setup/removal locked in H1; close proof refined in seq 7 |
| `finding_1786329053_226708` close oracle cites unavailable `connection_state()` | high | **Seq 7:** replace with production-path `peer.close()` **completion evidence** (`cfg(test)` counter inside strengthened forget). Do **not** use `connection_state()`. |

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for this run

## Other role/surface playbooks and atomic notes loaded

### Role / stack overlays

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-runtime-reviewer-playbook]] (downstream review overlay for daemon/transport/lifecycle)
- [[cli-patterns]]

### Targeted atomic notes / source captures

- [[terminal webrtc failure records do not prove peer runtime teardown]] — primary diagnosis
- [[webrtc peer cleanup removes every per peer owner together]] — complete ownership set for teardown
- [[file descriptor exhaustion from stale webrtc connections]] — related stale-peer resource family
- [[graceful-termination-requires-explicit-cleanup-hooks]] — explicit close, not Drop-only
- [[cleanup_webrtc_channel double-fires from concurrent callers]] — idempotent concurrent cleanup
- [[late webrtc messages after disconnect must not recreate clients]] — no post-terminal resurrection
- Source capture: `ops/archive/inbox/failed-local-webrtc-peer-can-spin-botster-hub-cpu-and-drain-battery.md`
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — package/plugin/workflow policy paths are out of scope
- Other repository charters — not the ticket target

## Context loaded

### Product problem

A long-lived debug `botster-hub` can peg multi-core CPU (~500%, ~5 hot `botster-local-webrtc` threads) for many hours after a local WebRTC `entity_delivery` peer reaches terminal `peer_failed`. Disk already shows a completed `local-webrtc-sender-terminal.json` with `peer_connection_state: failed` and `cause: peer_failed`, while live stacks stay in `PeerConnectionDriver` / `handle_timeout` / `poll_timeout` and ICE/DTLS/SCTP retransmit paths.

Terminal persistence and live peer/runtime teardown have diverged. Debug builds amplify cost; they are not the root cause.

### Code diagnosis (production path)

1. **Terminal signal** (`src/local_webrtc.rs`)
   - `LocalWebrtcHandler::on_connection_state_change` → `observe_peer_connection_state`
   - `RTCPeerConnectionState::Failed` → `LocalWebrtcTerminalCause::PeerFailed`
   - `cleanup_once` (idempotent via `cleanup_sent`) builds `LocalWebrtcSenderTerminalRecord` and sends `ControlMessage::LocalWebrtcPeerClosed`
   - Does **not** call `PeerConnection::close`

2. **Control-plane cleanup** (`src/daemon_transport.rs` `handle_control_message` arm `LocalWebrtcPeerClosed`)
   - lifecycle counters + **persist** `local-webrtc-sender-terminal.json`
   - `daemon.local_webrtc().remove_peer(&grant_id)`
   - detach entity/attach subscriptions

3. **Broken live teardown** (`LocalWebrtcTransport::remove_peer`)
   - map-only remove; no `peer.close()`; never parks dedicated runtime

4. **Working sibling** (`LocalWebrtcTransport::stop_all`)
   - close every peer via `runtime.block_on(peer.close())` then `runtime.take()`

### Placement authority

- Plans: `docs/plans/`
- Tests: `./test.sh` (`BOTSTER_ENV=test`, `--workspace`)

### Baseline verification (Plan, pre-implementation)

```text
HEAD 1c6a41e36f6f1bcc09d52feb4f61ae507b9bc04f
./test.sh --lib local_webrtc
  botster-hub lib: 22 passed, 0 failed (filter local_webrtc)
  hub-client: 3 passed
  (new H1/H2/H3 tests do not exist yet — that is the Implement gap)
```

## Botster layers touched

| Layer | In scope? |
| --- | --- |
| Hub local WebRTC transport / peer registry | **Yes** |
| Hub daemon control-plane `LocalWebrtcPeerClosed` | **Yes** |
| Core / hub-client / web / session workers / Project Pipelines | **No** |

## Worktree / target assumptions

- Implement and test only in the pipeline worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- No cross-repo dependency ticket for the primary fix.

## Scope

Surgical repair of **live** local-WebRTC peer teardown on terminal failure (including `peer_failed` / entity-delivery):

1. **One production teardown path** that both persists the terminal sender record **and** stops the live peer runtime.
2. On the sole production forget used by `LocalWebrtcPeerClosed`:
   - fully **close** the `PeerConnection` (stops `PeerConnectionDriver` timeout/retransmit loops);
   - remove the peer from the live peer map;
   - when peer count reaches zero, **immediately drop** the dedicated `botster-local-webrtc` tokio runtime (decision locked below).
3. Teardown remains **idempotent** under concurrent cleanup callers.
4. Late queued work after terminal failure cannot re-insert that peer or invent a second teardown path.
5. Prove the production cleanup path with the **named harnesses H1–H3** (not optional, not “Implement chooses”).

### Locked implementation decisions

| Decision | Choice |
| --- | --- |
| Where close happens | Only in transport forget invoked from `handle_control_message` → `LocalWebrtcPeerClosed` (same path as terminal persist). No second legacy close path. |
| Forget sequence | (1) `peers.remove(grant_id)` idempotent early-return if missing (2) `runtime.block_on(peer.close())` if dedicated runtime present (3) if `peers.is_empty()`, `self.runtime.take()` and drop |
| Runtime shutdown style | **Immediate take/drop** of the multi-thread runtime after last-peer close — same semantics as `stop_all`, applied to normal peer churn. Not a multi-phase graceful shutdown API. |
| Runtime recreate | Next `signal()` / `runtime()` builds a new multi-thread runtime (existing pattern). |
| `stop_all` | Share the close helper with single-peer forget so shutdown and churn cannot drift. |
| Close-specific test oracle | **Not** `connection_state()` (unavailable on consumed `webrtc` 0.20.0-rc.1 `PeerConnection`). Use **production-path close-completion evidence** recorded inside the same strengthened forget that H1 exercises (see H1). |
| DTO / public API | Unchanged |

### Implementation sketch

```text
LocalWebrtcTransport::remove_peer / close_and_remove_peer  (single method; sole LocalWebrtcPeerClosed call site)
  let Some(peer) = peers.remove(grant_id) else { return };
  if let Some(runtime) = runtime.as_ref() {
      let close_result = runtime.block_on(peer.close());
      // Production: ignore already-closed / benign close errors (same spirit as stop_all).
      // #[cfg(test)] only: record close-completion evidence for grant_id when close() was
      // invoked and returned Ok (or a classified already-closed success). Do NOT record if
      // the close() call was skipped. This is the only close-specific H1 oracle.
  }
  if peers.is_empty() { let _ = runtime.take(); }

handle_control_message(LocalWebrtcPeerClosed):
  persist terminal record   // existing
  transport.close_and_remove_peer(grant_id)  // fixed
  detach entity_subscription_ids + attach subscriptions  // existing
```

`cleanup_once` stays the only producer of `LocalWebrtcPeerClosed` for terminal causes (including `PeerFailed`).

#### Close-completion evidence (locked design)

| Rule | Detail |
| --- | --- |
| Where recorded | Inside the **production** forget method body, immediately after `block_on(peer.close())`, under `#[cfg(test)]` only |
| What is recorded | At minimum: `grant_id` + that `close()` was **invoked and completed** on the production path (e.g. push to a `Mutex<Vec<…>>` or increment per-grant `AtomicU64` on `LocalWebrtcTransport`, with `pub(crate)`/`cfg(test)` take/inspect helper) |
| What is **not** allowed | Recording close success from a test double that never calls the real `webrtc` `PeerConnection::close`; inventing a second production close path only for tests; citing `connection_state()` / stats connection state (not on trait / stats in 0.20.0-rc.1) |
| Production builds | Zero runtime cost / no durable fields outside `cfg(test)` (or `#[cfg(test)]` fields on the transport) |

## Non-scope

- Session-worker / plugin-thread capacity work
- Browser UI, ICE/TURN redesign, debug-vs-release as the fix
- Broad WebRTC refactors, optional configurability, new metrics platforms
- Cancelling or merging the parallel comparison ticket
- Project Pipelines package work

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner |
| --- | --- |
| Local WebRTC peer map, dedicated runtime, terminal cleanup policy | **botster-hub** |
| `webrtc` crate PeerConnection mechanics | external crate (call `close()`) |
| Client DTOs / browser | out of scope |

**Cross-repo dependencies:** none. Escalate to Core only if `PeerConnection::close` is proven insufficient to stop the driver after map ownership is released.

## Assumptions and unknowns

### Assumptions

1. `PeerConnection::close` stops that peer’s `PeerConnectionDriver` timeout loops (`stop_all` already relies on this).
2. Dropping the dedicated runtime when the map is empty ends `botster-local-webrtc` worker threads for that runtime instance.
3. `LocalWebrtcPeerClosed` runs on the hub control/owner thread that already uses `runtime.block_on` for `signal()`, so per-peer `block_on(close)` is safe there.
4. Injecting `RTCPeerConnectionState::Failed` through the **production** `on_connection_state_change` / `observe_peer_connection_state` code path is a valid deterministic `peer_failed` trigger; waiting for real ICE failure is not required and must not be the only proof.
5. The live peer under that trigger is a **real** `webrtc` `PeerConnection` created by production `signal()` / `answer_offer`, not a close-recording test double as the sole subject under test.

### Resolved former unknowns (no longer open to Implement choice)

| Former unknown | Resolution |
| --- | --- |
| How to force `peer_failed` | Harness **H1** trigger below |
| Graceful vs immediate runtime drop | Immediate `runtime.take()` after last peer (locked) |
| Whether JSON alone suffices | Explicitly **no** — H1 oracles require live map, runtime, threads, and terminal file |

### Remaining escalate-only unknowns

1. If real `close()` + runtime drop still leaves non-hub threads spinning inside the `webrtc` crate after H1 fails, stop and re-plan (possible crate bug / extra hold). Do not paper over with sleeps.

## Affected surfaces/files

| Path | Change |
| --- | --- |
| `src/local_webrtc.rs` | Close-on-forget; empty-map runtime drop; `pub(crate)`/`cfg(test)` live oracles (`active_peer_count`, `has_dedicated_runtime`); **`#[cfg(test)]` close-completion evidence** on forget; **H1–H3** tests |
| `src/daemon_transport.rs` | Call strengthened forget from sole `LocalWebrtcPeerClosed` arm; keep persist order: prefer **close/remove/park then or after** persist without dual paths — recommended order: persist then close (matches today) or close then persist if needed for crash safety; either is fine if both happen in the same arm |
| `src/daemon.rs` | Only if sharing `stop_all` helper |
| `tests/hub_daemon_lifecycle_test.rs` | **Not required** if H1–H3 fully exercise production `signal` + `LocalWebrtcPeerClosed` forget in-process. Optional loaded daemon smoke only if unit harness cannot create a real peer |
| `docs/plans/stop-failed-local-webrtc-peers-from-spinning-hub-cpu.md` | This plan |
| `docs/reports/…` | Implement report later |

## Risks

| Risk | Mitigation |
| --- | --- |
| Close races with in-flight send/poll | Idempotent close; existing `cleanup_sent` serializes `LocalWebrtcPeerClosed` |
| `block_on(close)` from wrong thread | Keep forget on control-plane path only |
| Dropping runtime while sibling peer lives | Only `take()` when map empty (**H2**) |
| Hub control path hangs on close | **H3** proves new `signal()` after last-peer cleanup within a bound |
| Test double only | **H1** forbids mock-only subjects |
| Runtime drop without close green-washes H1 | Close-completion evidence only recorded after real `close()`; red proof axis 2 |
| Unavailable `connection_state()` | Do not use it; use close-completion evidence (seq 7) |
| H1 without entity-delivery | Setup+teardown oracles required for ticket entity-delivery case |
| Parallel ticket divergence | Documented; human authorized independent comparison |

## Acceptance checks / tests

### Success criteria (ticket)

1. After `peer_failed` / terminal cleanup on an **entity-delivery** local peer, no live ownership of a hot `PeerConnectionDriver` timeout loop for that peer in hub transport state.
2. Peer gone from live peer map; dedicated runtime dropped when peer count is zero; **entity subscription removed** from daemon entity subscription state.
3. Production forget **invoked and completed** `PeerConnection::close` for that grant (close-completion evidence), not merely map removal or runtime drop.
4. Hub control path remains usable; idle multi-core peg after failure is eliminated (thread + close-completion oracles in H1; long battery soak is observational, not the CI gate).
5. Verification inspects live peer/runtime/thread/**close-completion**/entity state **and** terminal JSON — JSON alone fails review; runtime drop alone without close fails review.
6. Focused tests H1–H3 prove the production cleanup path (names fixed below).
7. One teardown path: terminal persist + live stop + entity detach in the `LocalWebrtcPeerClosed` arm.

### Required harness H1 — peer_failed production-path proof (entity-delivery + close)

**Exact test name (Implement must use this name or an exact rename recorded in the implement report with the same behavior):**

`local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads`

This is the ticket’s **entity-delivery** peer_failed case, not a generic bare peer.

#### Production chain (must all run)

```text
real signal / answer_offer
  → real DataChannel open
  → SubscribeEntities (entity-delivery peer) accepted
  → on_connection_state_change(Failed)   // production handler path
  → observe_peer_connection_state(Failed)
  → cleanup_once(PeerFailed)             // carries entity_subscription_ids
  → ControlMessage::LocalWebrtcPeerClosed
  → handle_control_message arm:
       persist local-webrtc-sender-terminal.json
       transport close + map remove + empty runtime drop
       remove entity_subscription_ids from daemon entity_subscriptions
```

| Element | Specification |
| --- | --- |
| **Production entry point** | Full chain above. Forget that closes the peer is the **same method** called by the `LocalWebrtcPeerClosed` arm (sole production call site). Helper-only close that bypasses that arm fails review. |
| **peer_failed trigger** | Deterministic call into the **production** `on_connection_state_change` / `observe_peer_connection_state` path with `RTCPeerConnectionState::Failed`. Do **not** rely on flaky real ICE failure as the only trigger. Do **not** skip `cleanup_once` / `LocalWebrtcPeerClosed`. |
| **Live subject** | Real `webrtc` `PeerConnection` from production `LocalWebrtcTransport::signal` / `answer_offer` (real offer SDP; reuse `LocalWebrtcOfferPeer::create_offer` patterns). A mock that only records `close()` is **insufficient** as the sole subject. |
| **Entity-delivery setup (required before failure)** | After the peer is live, establish a **real entity-delivery subscription** on that peer using the production subscribe path used by local WebRTC (encrypted DataChannel `DaemonRequest::SubscribeEntities` → `ControlMessage::SubscribeEntities` → register in `state.entity_subscriptions`, and peer `add_entity_subscription` after `EntitySubscribed`). Prefer the same request shape lifecycle tests already use (`SubscribeEntities` with a concrete `entity_type` / `subscription_id`). **Precondition assert:** daemon `entity_subscriptions` contains `subscription_id` and peer-owned entity id set contains it (or `live_entity_subscriptions >= 1` with that id present). Pattern reference: existing unit `entity_subscription_multiplexes_after_ack_and_cleans_up_with_peer` and lifecycle WebRTC `SubscribeEntities` flows. |
| **Entity-delivery teardown oracle (required)** | After `LocalWebrtcPeerClosed` processing: that `subscription_id` is **absent** from daemon `entity_subscriptions`; `live_entity_subscriptions` decreased accordingly; the closed message’s `entity_subscription_ids` included that id (prove cleanup_once collected it). Failing to register a subscription before failure makes H1 invalid. |
| **Live peer subject (required)** | Real `Arc<dyn PeerConnection>` from production `signal` / `answer_offer` remains the peer under forget. Retaining a clone for post-checks is optional; **do not** require observing `connection_state()` on it (API does not exist on consumed trait). |
| **Close-specific oracle (required, feasible)** | After production forget, H1 asserts **close-completion evidence** for that `grant_id` was recorded by the strengthened forget path: `peer.close()` was invoked via the dedicated runtime and completed (Ok / already-closed success). Evidence lives only under `#[cfg(test)]` on `LocalWebrtcTransport` (or a `pub(crate)` test helper that reads it). Terminal JSON still records the failure disposition (`cause == "peer_failed"`, `peer_connection_state == "failed"`) — pre-close terminal snapshot, **not** a substitute for close-completion evidence. |
| **Forbidden close oracles** | `PeerConnection::connection_state()`, stats-based connection state, mock-only “close was called” without real `webrtc` peer + production forget body, or any oracle that passes when map remove + runtime drop run but `close()` is skipped. |
| **Terminal-file oracle (required)** | Read `local-webrtc-sender-terminal.json` (production path constants). Assert `grant_id` match, `cause == "peer_failed"`, `peer_connection_state == "failed"`. No “when applicable”. |
| **Peer/runtime state oracle (required)** | `active_peer_count() == 0` and `has_dedicated_runtime() == false`. |
| **Timeout-driver / thread oracle (required)** | Within ≤ 2s: no OS threads named `botster-local-webrtc` remain (or dedicated runtime workers fully joined). Complements close-completion; does not replace it. |
| **Precondition** | Peer count 1; dedicated runtime present; ≥1 `botster-local-webrtc` thread; entity subscription registered as above; close-evidence buffer empty or baseline taken. |
| **Red proof (required, multi-axis)** | Document in implement report per [[a regression test must be shown to go red with the fix reverted]]: |
| | 1. **Map-only remove** (today’s bug): H1 fails (runtime and/or threads and/or missing close-completion). |
| | 2. **Map remove + runtime drop without `peer.close()`**: H1 **must still fail** on the **close-completion** oracle (evidence absent for grant). Runtime/thread oracles alone must not green-wash a missing `close`. |
| | 3. Optional third axis: skip entity-subscription removal in the control arm → entity teardown oracle fails. |

### Required harness H2 — sibling peer survives single-peer cleanup

**Test name:** `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime`

1. Signal **two** real local peers (two grants) into one `LocalWebrtcTransport` / one daemon local_webrtc state.
2. Trigger terminal `peer_failed` cleanup for peer A only through the same production chain as H1.
3. Assert: peer A gone; peer B still in map; dedicated runtime **still present**; peer B still closable or signal-state intact (map membership + runtime alive is the minimum; optional second operation on B if cheap).
4. Bound: completes without hang (same control-path timeout budget as H1).

### Required harness H3 — runtime reuse after last-peer park

**Test name:** `local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds`

1. After last-peer cleanup (map empty, runtime dropped) as in H1.
2. Issue a **new** bootstrap grant and production `signal()` with a new real offer.
3. Assert within bound: answer succeeds, `active_peer_count() == 1`, `has_dedicated_runtime() == true`, control path did not deadlock.
4. Cleanup the new peer so the test does not leak runtimes.

### Commands (Implement / Verify)

```sh
# Required focused proofs (exact names):
./test.sh local_webrtc_peer_failed_closes_live_peer_parks_runtime_and_clears_driver_threads
./test.sh local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime
./test.sh local_webrtc_after_last_peer_cleanup_new_signal_recreates_runtime_and_succeeds

# Broader surface:
./test.sh --lib local_webrtc
./test.sh local_webrtc
```

### Downstream proof (hub charter)

- Repo-owned `./test.sh` only.
- Plan Review / Verify load [[botster-runtime-reviewer-playbook]] / matching verifier.
- No package live-binary campaign required for this ticket.

### Runtime-path proof summary

```text
SubscribeEntities (entity-delivery on live peer)
  → on_connection_state_change(Failed)
  → cleanup_once(PeerFailed)  // includes entity_subscription_ids
  → ControlMessage::LocalWebrtcPeerClosed
  → persist local-webrtc-sender-terminal.json
  → transport: PeerConnection::close (record cfg(test) completion) + map remove + empty runtime drop
  → daemon entity_subscriptions remove for carried ids
```

Evidence that a close helper exists without this chain, or that runtime drop alone empties threads without close-completion evidence, is **not** acceptance.

### Assumptions update (API feasibility)

1. Consumed crate is `webrtc` **0.20.0-rc.1** with `PeerConnection::{close, get_stats}` only for close/stats — **no** `connection_state()`.
2. `runtime.block_on(peer.close())` is the correct production close call (matches `stop_all`).
3. `#[cfg(test)]` close-completion evidence inside that forget body is sufficient and necessary for H1’s close-specific red axis; Implement must not invent `connection_state` wrappers.

## Pipeline gates and artifacts

| Item | Value |
| --- | --- |
| Gate | `bsg_plan_gate` (attestation) |
| Artifact | this plan under `docs/plans/` |
| Next step | `bsg_plan_review` |
| Prior reviews | `review_1786328360_187546`, `review_1786328761_521864`, `review_1786329053_392900` (close oracle API infeasible) |

## Vault gaps worth capturing

1. **Map-remove without `peer.close()` is not teardown** (hub-specific).
2. **Dedicated multi-thread runtime must drop when peer count hits zero** under normal churn, not only `stop_all`.
3. After ship: update [[terminal webrtc failure records do not prove peer runtime teardown]] with fix evidence rather than many new notes.

**Plan-time capture decision:** no new vault note yet — source capture + atomic note already hold the diagnosis. Capture after Implement with green H1 red-proof if the fix reveals a new mechanism.

## Product decision ledger

| Decision | Default |
| --- | --- |
| Close location | Transport forget from `LocalWebrtcPeerClosed` only |
| Runtime lifecycle | Create on demand; **immediate drop** when last peer removed |
| Public API | Unchanged |
| Parallel ticket | Continue independently (human) |
| Ask-human | Only if close insufficient or Core ownership required |

## Checklist for Implementer

1. Read this plan + [[botster-hub-playbook]] + [[terminal webrtc failure records do not prove peer runtime teardown]].
2. Implement close+remove+empty-runtime-drop on the sole `LocalWebrtcPeerClosed` forget path; share with `stop_all`.
3. Implement H1–H3 with the exact names and oracles above; show H1 red with fix reverted.
4. Run the three focused `./test.sh` filters + `./test.sh --lib local_webrtc`.
5. Write implement report under `docs/reports/`.
