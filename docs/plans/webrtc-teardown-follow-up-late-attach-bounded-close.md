# WebRTC teardown follow-up: late Attach admission, bounded close, and live no-spin oracle

## Plan revision

| Field | Value |
| --- | --- |
| Revision | sequence 5 — Plan Review `review_1786387173_526030` ownership-matrix remediation |
| Prior review | sequence 3 resolved `review_1786386846_575058` (context, hang deadline, structured gate, worker build) |
| Open findings addressed now | `finding_1786387173_497133`, `finding_1786387173_404496` |

### Plan Review findings → plan response

| Finding | Severity | Resolution |
| --- | --- | --- |
| Required Botster and Hub context incomplete | high | Resolved seq 3 — full charter notes with plan-impact table |
| Hung-close proof lacks production-handler completion deadline | high | Resolved seq 3 — acceptance check 4 |
| Plan gate omits structured teardown evidence | high | Resolved seq 3 — discrete lens fields in gate |
| Baseline omits session-worker build precondition | medium | Resolved seq 3 — worker build before `./test.sh local_webrtc` |
| **Late Unsubscribe can delete a replacement owner's row** | high | **Seq 5:** owner-checked Unsubscribe when grant not live; closed-A / reused-by-B / late-Unsubscribe-A test **mandatory** |
| **Peer-originated Request admission remains optional** | high | **Seq 5:** **every** `ControlMessage::Request` with `grant_id: Some` fails closed when grant not live; late Spawn (or equivalent durable non-Attach) test **mandatory** |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` → path from `list_spawn_targets` (not ambient cwd) |
| Pipeline worktree | branch `project-pipelines/ticket_1786385940_304814` |
| Ticket | `ticket_1786385940_304814` |
| Run | `run_1786386094_979398` |
| Plan step | `botster_stack_plan` sequence 5 (`run_step_1786387186_239901`) |
| Base ref | `main` after PR **#200** merge (`26f1673` and descendants) |
| Dependency | `ticket_1786327694_445993` (closed) — parent battery/CPU fix / PR #200 |
| Closed dual arm (ideas only) | `ticket_1786324642_480494` / PR **#199** — do not merge; borrow close-bound ideas |

Resolved via `list_spawn_targets` against the ticket/run `target_id`. Do **not** infer ownership from ambient process cwd.

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for this run

## Other role/surface playbooks and atomic notes loaded

### Role / stack overlays

| Note | Plan impact |
| --- | --- |
| [[planner-playbook]] | Role contract: scope, assumptions, risks, acceptance |
| [[botster-planner-playbook]] | Botster layers, worktree binding, runtime-teardown class trigger |
| [[botster-architecture]] | Domain map; confirms hub vs core vs client ownership; no new surfaces |
| [[cli-patterns]] | Rust hub/CLI patterns; reinforces control-plane vs data-plane; no architecture change |
| [[botster runtime teardown lenses]] | **Required class overlay** — all lens fields answered below |
| [[botster-runtime-reviewer-playbook]] | Downstream Review overlay (named, not Plan execution) |
| [[botster-runtime-verifier-playbook]] | Downstream Verify overlay (named, not Plan execution) |

### [[botster-hub-playbook]] Must Load notes (complete)

| Note | Plan impact |
| --- | --- |
| [[botster hub is a first party host profile over core]] | Confirms Hub owns control-plane cleanup/admission; change stays in hub |
| [[botster hub gravity must be watched before it becomes the new monolith]] | Surgical change only; no new hub monolith surface |
| [[botster data plane bypasses the hub through session and client actors]] | No SessionIo/ClientWorker byte-path edits; attach admission is control-plane only |
| [[botster local client api lives over hubruntime not raw core routers]] | No new client facade; Attach remains existing daemon request |
| [[botster hub events use bounded priority lanes instead of unbounded queue fuses]] | Close hang on control thread is a related bound problem — plan adds close timeout |
| [[may supervise permits the hub to supervise the package entrypoint]] | No package supervision change |
| [[hub supervision admission changes require exact live hub launch proof]] | Not package admission; no live-hub-binary package proof required for this ticket |
| [[live hub proof records distinct hub and locked core binary provenance]] | Focused tests use lockfile-pinned core worker build; Verify live path (if any) must record provenance if exact hub binary is claimed |
| [[webrtc bootstrap origin must be requested after the package server binds]] | No bootstrap/origin grant issuance change |
| [[plugin worker queue capacity and executor concurrency are independent host profile knobs]] | No plugin-worker resource knobs |
| [[package entity hydration uses explicit providers not mcp naming]] | No package entity providers |
| [[durable state version preflight must precede shape deserialization after cold turkey changes]] | No HubState schema change |
| [[hub qualifies effective session type ids as source name slash id]] | No session-type projection change |
| [[sanitized projection plus wholesale replacement update contracts silent data loss]] | No editor projection DTOs |
| [[editor scoped reads sit in the mutation admission group not the sanitized read group]] | No editor authorization |
| [[hub drain advances non attached session lifecycle]] | Drain remains available for live peers; late Drain after PeerClosed fails closed with other WebRTC Requests when grant gone |
| [[hub shutdown preserves durable session workers]] | Peer teardown is not hub process stop; session workers still need explicit ShutdownSession/RemoveSession (existing harness cleanup) |
| [[botster runtime teardown lenses]] | Class applies; full answers in lens section |
| [[sessionio hard socket death must fan process exited to clientworkers]] | Data-plane death fanout unchanged; hub control-plane attach admission only |
| [[terminal pending attach is only for missing handlecache sessions]] | No HandleCache pending-queue change; late Attach reject is grant/live-peer, not pending attach |
| [[incomplete repo local session types drop the hub client connection]] | No session-type validation path; typed operator error pattern reinforces peer-gone OperatorError without dropping transport |

### Targeted atomic notes

| Note | Plan impact |
| --- | --- |
| [[terminal webrtc failure records do not prove peer runtime teardown]] | Live oracles required; terminal JSON insufficient |
| [[webrtc peer cleanup removes every per peer owner together]] | Attach residual is part of ownership set |
| [[file descriptor exhaustion from stale webrtc connections]] | Related resource family; close must complete |
| [[late webrtc messages after disconnect must not recreate clients]] | Attach is the missing ownership-creating surface |
| [[graceful-termination-requires-explicit-cleanup-hooks]] | Explicit close + bound, not Drop-only |
| [[test script required for rust tests not cargo test]] | `./test.sh` only |
| [[a regression test must be shown to go red with the fix reverted]] | Red-on-revert for late Attach and/or close bound |

### Explicitly not loaded

- [[project-pipelines-playbook]] — package/plugin/workflow policy paths out of scope
- Other repository charters — not the ticket target
- [[botster-package-reviewer-playbook]] — no package/plugin surface change

## Context loaded

### Product problem (follow-up to #200)

PR **#200** landed the production forget path: on `LocalWebrtcPeerClosed`, close the peer, remove it from the live map, park/drop the dedicated runtime when empty, grant-guard late `SubscribeEntities` / `UnsubscribeEntities`, owner-checked entity sweeps, and fail-closed sibling teardown when close ultimately fails. In-process close-completion counters and dedicated-runtime worker-thread joins prove much of the hard stop.

Residual gaps remain:

1. **Late Attach admission** — Entity subscribe/unsubscribe control messages carry `grant_id` and refuse recreation after peer map removal. `ControlMessage::Request` (including `DaemonRequest::Attach`) does **not** carry a grant tag and is not fail-closed against a gone peer. A queued Attach after terminal failure can create attach ownership that nothing owner-sweeps.
2. **Unbounded close** — Production `close_peer_on_runtime` still uses unbounded `runtime.block_on(peer.close())` (with one retry). A hung library close can stall the hub control plane; #200 only handles `Err` outcomes, not hang.
3. **Live no-spin oracle** — unit/integration proofs are strong for map empty + worker join, but the original multi-hour battery claim still wants an optional Verify-stage live path (offerer connect → fail/kill → CPU/sample bounds), not terminal JSON alone.
4. **Ownership edges** — entity reused-id + grant-owned empty-snapshot sweeps exist; attach residual ownership lacks a parallel owner-grant index on the control plane.
5. **Sibling fail-closed policy** — documented and tested for ultimate close **Err**; must remain explicit when hang → timeout → fail-closed.

### Code diagnosis (main after #200)

**Entity late-message (done):**

- WebRTC path stamps `grant_id: Some(peer_state.grant_id)` on `ControlMessage::SubscribeEntities` / `UnsubscribeEntities` (`src/local_webrtc.rs` `run_data_channel`).
- Control plane rejects subscribe when `!has_live_peer(grant_id)` with `local_webrtc_peer_gone` (`src/daemon_transport.rs`).
- PeerClosed owner-sweeps entity rows by `owner_grant_id` even when the peer snapshot was empty; preserves replacement owners for reused subscription ids.

**Attach / Request (gap):**

- WebRTC non-entity requests send `ControlMessage::Request { request, reply_tx, response_delivery_rx }` with **no** `grant_id`.
- Socket path also uses `Request` with no grant (correct for socket; admission must stay `None` = unrestricted).
- Successful Attach records via `record_attached_subscription_change` and `pending_runtime.active_subscriptions` **without** `owner_grant_id`.
- PeerClosed detaches attaches only from (a) peer cleanup snapshot + (b) fail-closed sibling peer_state take. If Attach lands on the control plane after snapshot and after peer_state removal, residual active attach can remain.

**Close path (gap):**

```text
remove_peer → close_peer_on_runtime → runtime.block_on(peer.close())  // unbounded
```

On `Err` after retry: quarantine + `fail_closed_drop_dedicated_runtime` (siblings die; tested). On hang: control thread blocks indefinitely.

**PR #199 borrow (not architecture rewrite):**

- Per-peer runtime + `tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, peer.close())` with ~3s bound.
- This plan keeps #200’s **shared dedicated multi-peer runtime** + fail-closed drop; ports only the **close bound** idea (timeout → treat as ultimate failure → existing fail-closed path). No dual full pipeline; no per-peer-runtime rewrite unless Implement proves shared runtime cannot meet bounds + admission (ticket out-of-scope default: do not rewrite).

### Botster layers touched

- Rust hub control plane (`daemon_transport`) and local WebRTC transport (`local_webrtc`) only.
- Not: Lua plugins, SPA, Rails relay, TUI, core actor byte plane, package entrypoints.

### Worktree / target assumptions

- Implement runs in this pipeline worktree on hub `target_id` above.
- Base is post-#200 main; do not re-land #200.
- Tests via `./test.sh` (sets `BOTSTER_ENV=test`, `--workspace`).
- Spawn-bearing tests require prebuilt session worker (README):
  `cargo build --locked -p botster-core --bin botster-session-worker`

## Scope

### In scope (must ship)

1. **Complete late-message matrix (mandatory, not optional)**
   - Add optional `grant_id: Option<String>` to `ControlMessage::Request` (mirror SubscribeEntities).
   - WebRTC `run_data_channel` stamps `Some(grant_id)` for **every** peer-originated Request.
   - Socket / non-WebRTC constructors leave `None` (unchanged admission).
   - **Mandatory:** when `grant_id: Some(g)` and `!has_live_peer(g)`, **reject every** `ControlMessage::Request` with typed `local_webrtc_peer_gone` (or same family) **before** `handle_control_request`. Covers Attach, Spawn, Detach, SendInput, Drain, session/package mutations, Status, and any future variant — no "preferred" subset. Rationale: any peer-originated Request after terminal failure is stale transport residue; several variants create durable ownership (Attach, Spawn, CreateSpawnTarget, package config, etc.).
   - On Attach admission success, record grant ownership so PeerClosed can sweep residual attach rows even when peer-side snapshot missed them.
   - Peer-side `apply_subscription_change` must not create durable attach bookkeeping on failed/stale responses (only on success).
   - **Mandatory late Unsubscribe owner check (bug on main after #200):** in `UnsubscribeEntities` when `grant_id: Some(g)` and peer not live, **do not** blindly `remove(subscription_id)`. Remove only when:
     - no row exists (no-op), or
     - `owner_grant_id` is `None` (unowned — explicit policy: allow residual cleanup), or
     - `owner_grant_id == Some(g)` (stale grant still owns the row).
     If `owner_grant_id` is `Some(other)` (replacement owner B after id reuse), **preserve** the row and counters; still reply `EntityUnsubscribed` for stale-client idempotency (or OperatorError if Implement proves reply shape is safer — default: preserve B + idempotent unsubscribed reply for A). Live-peer Unsubscribe path continues to remove by id as today when the sending peer is live.

2. **Bounded teardown on production forget path**
   - Wrap `peer.close()` waits in a hard time bound (start from #199’s ~3s; named const, e.g. `LOCAL_WEBRTC_PEER_CLOSE_BOUND`).
   - Timeout (or hang) is treated as ultimate close failure → existing fail-closed sibling teardown + runtime drop.
   - Do not leave unbounded `block_on(close)` as the only control-thread wait on the forget path.

3. **Production-path proof (tests)** — see Acceptance checks (includes hang-handler deadline).

4. **Sibling / fail-closed policy (explicit)**
   - Success close: only primary grant removed; siblings stay live.
   - Ultimate close failure **or timeout**: fail-closed drops dedicated runtime and all remaining peers; sibling attach/entity ownership cleared; document + test hang path as well as Err path.

5. **Optional Verify live no-spin**
   - Preferred: offerer connect → peer_failed / kill → sample CPU or assert no hot `botster-local-webrtc` workers beyond idle join oracle over a short window.
   - If impractical in CI, Implement/Verify must land a focused harness **or** register a dependency ticket / human waiver with residual battery risk named.

### Non-scope

- Replacing shared dedicated runtime with full per-peer runtime rewrite (default no).
- Dual full pipelines / second planner arm.
- Re-implementing #200 map-remove / park / entity grant guards.
- Unrelated hub polish, package/plugin work, client DTO ownership, core SessionIo byte path.
- Merging or rebasing PR #199 as a unit.
- Workspaces plugin UiNode rendering issues (unrelated ambient UI).

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This run |
| --- | --- | --- |
| Local WebRTC peer map, dedicated runtime, control forget path | **botster-hub** | Yes |
| Daemon control admission for Attach / entity subs | **botster-hub** | Yes |
| SessionIo / ClientWorker terminal bytes | core / data plane | No change |
| Hub-client DTOs | botster-hub-client (in-tree crate OK if error shape already shared) | Prefer reuse existing OperatorError codes |
| Browser / TUI / Workspaces clients | other repos | No |

**Cross-repo prerequisites:** none registered. Parent dependency `ticket_1786327694_445993` is closed. Live no-spin, if deferred, becomes a **same-repo** follow-up dependency ticket or human waiver (not a silent skip).

## Assumptions and unknowns

### Assumptions

1. Post-#200 main is the correct base; worktree includes merge of #200.
2. Shared dedicated multi-peer runtime remains the architecture; timeout + fail-closed is sufficient without per-peer runtimes.
3. `local_webrtc_peer_gone` (or same code family) is the right typed error for late Attach.
4. Socket path must remain untagged (`grant_id: None`) and fully functional.
5. ~3s close bound from #199 is a reasonable starting constant.
6. In-process worker-thread join remains valid hard-stop oracle; live CPU sample is preferred Verify stretch, not a silent blocker if waived with ticket.
7. README session-worker build is required before any focused test that Spawns/Attaches a real session.

### Unknowns (Implement resolves with evidence)

1. ~~Which Request variants fail closed~~ — **decided seq 5:** all tagged Requests fail closed when grant not live.
2. Minimal attach owner index shape.
3. Hang injection design that exercises **production** `close()` wait without flaking CI.
4. Live no-spin feasibility in Verify CI time budget.
5. Stale Unsubscribe reply shape when preserving B's row (default: EntityUnsubscribed for A idempotency).

## Affected surfaces / files

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | `ControlMessage::Request { grant_id }`; **mandatory** live-peer fail-closed for all tagged Requests; **owner-checked** late Unsubscribe; PeerClosed attach owner sweep; constructors (socket `None`) |
| `src/local_webrtc.rs` | Stamp grant on Request; close timeout in `close_peer_on_runtime`; peer-side attach apply only on success; tests (late Attach, late Spawn, late Unsubscribe reuse, hang inject) |
| `docs/plans/webrtc-teardown-follow-up-late-attach-bounded-close.md` | This plan |
| Possibly `docs/reports/` at Implement/Verify | Evidence only |

## Runtime-teardown lens answers

| Field | Content |
| --- | --- |
| `teardown_class_applies` | **yes** — WebRTC peer lifecycle, control-plane forget/close, multi-peer shared runtime, CPU/battery spin class, terminal-state vs live-runtime divergence |
| `teardown_isolation` | Ownership set for one failed peer: live map entry, peer_state (attach + entity id lists), grant-owned daemon entity rows, grant-owned attach rows (new), terminal record path. Shared resource: one multi-thread dedicated `botster-local-webrtc` runtime for all local peers. Successful close isolates to that grant. Ultimate close failure/timeout **sacrifices siblings** by dropping the shared runtime (existing #200 policy). Prefer improving hang bound first; full per-peer isolation is non-scope unless bounds fail. |
| `teardown_bounds` | Production forget must not unbounded-block on `peer.close()`. Bound wait (≈3s start, named const); on timeout or final Err → fail-closed drop dedicated runtime (hard stop for driver loops). Park path when map empty still drops runtime. **Handler completion:** production `handle_control_message(LocalWebrtcPeerClosed)` / `remove_peer` must return within a named test deadline under forced hang (see acceptance check 4). |
| `late_message_matrix` | See table below |
| `production_path_proof` | Terminal signal → `cleanup_once` → `LocalWebrtcPeerClosed` → `handle_control_message` → `remove_peer` (bounded close + map remove + park/fail-closed) → control sweeps. Tests drive real handler path (not helper-only). Oracles: map empty, no live peer, runtime parked, worker threads join, **handler returns by deadline under hang**, close completion where close succeeds, late Attach/Subscribe fail closed, residual attach/entity absent. Optional Verify: CPU/no-spin sample. |
| `ownership_identity` | Entity rows: `owner_grant_id` (exists). Attach rows: add grant ownership for WebRTC-created attaches. Delayed PeerClosed must not delete attach/entity now owned by a different live grant. **Late Unsubscribe from stale grant must not delete replacement owner's row** (owner-checked). Owner sweeps cover closed-first and message-first queue orders. |
| `sibling_fail_closed_policy` | **Success close:** siblings keep working. **Ultimate failure or close timeout:** all peers on the dedicated runtime are closed best-effort and runtime is dropped; sibling entity + attach ownership cleared; existing fail-closed test extended for timeout/hang path. |

### Late-message matrix

| Message / ownership surface | Grant/owner tag | Reject after terminal | Residual sweep / owner rule |
| --- | --- | --- | --- |
| `SubscribeEntities` | `grant_id` + row `owner_grant_id` | `local_webrtc_peer_gone` when grant not live | PeerClosed snapshot + owner_grant sweep (exists) |
| `UnsubscribeEntities` | `grant_id` | When peer not live: **owner-checked** cleanup only (see scope); never delete replacement owner's row | PeerClosed still owner-sweeps by grant; late Unsub must match owner or no-op on foreign ownership |
| `Request` (all variants, WebRTC) | **Mandatory** `grant_id: Some` stamp | **Mandatory** peer-gone fail-closed for **every** variant when grant not live | Attach residual: grant-owner sweep on PeerClosed; Spawn/other never run after terminal |
| `Request` / Attach | as above + attach grant ownership | peer-gone; no attach row | PeerClosed owner sweep even if peer snapshot empty |
| Socket `Request` / entity paths | `grant_id: None` | No live-peer gate | Connection cleanup path (unchanged) |

**Durable Request examples that must not run after PeerClosed** (covered by universal tagged-Request gate, not special-cased): `Attach`, `Spawn`, `Detach` (no-op vs residual via PeerClosed sweep), `CreateSpawnTarget` / session-type mutations, package enable/config, `SendInput`/`Drain`/`Status` (stale residue). **Test at least:** late Attach + **late Spawn** (non-Attach durable) + late Unsubscribe reuse.

## Implementation sequence (for Implement)

1. Extend `ControlMessage::Request` with `grant_id: Option<String>`; fix all constructors/match arms/tests.
2. WebRTC path stamps grant; control plane **rejects all** tagged Requests when grant not live (mandatory universal gate before `handle_control_request`).
3. Fix late `UnsubscribeEntities` owner check when peer not live (preserve replacement owners).
4. Attach owner bookkeeping + PeerClosed residual attach sweep.
5. Bound `close_peer_on_runtime`; map timeout → `ClosePeerOutcome::Failed` / fail-closed.
6. Hang inject + handler join deadline test (acceptance check 4).
7. Tests: late Attach, **late Spawn**, **late Unsubscribe reuse (A closed, B owns id)**, attach empty-snapshot sweep, hang/timeout fail-closed; keep #200 suite green.
8. Precondition: `cargo build --locked -p botster-core --bin botster-session-worker` then `./test.sh local_webrtc`.
9. Verify: production-path evidence + optional live no-spin or explicit waiver/dependency.

## Risks

| Risk | Mitigation |
| --- | --- |
| Grant-tagging all Requests breaks a socket or internal constructor | Keep `None` default; audit every constructor site |
| Over-rejecting Request variants breaks legitimate post-signal races while peer still live | Gate on `has_live_peer` only (same as entity); race tests for attach-first and closed-first |
| Close timeout too aggressive causes false fail-closed (kills healthy siblings) | Start ~3s; only on forget path; document sibling sacrifice; multi-peer success isolation test |
| Close timeout too long still hurts control plane | Bound is finite; better than hang |
| Forced hang hangs the **test** itself | Run production handler behind thread join / channel with **named deadline** larger than close bound but finite; assert return before postconditions |
| Live no-spin deferred silently | Ticket requires dependency or human waiver with residual risk |
| Scope creep into per-peer runtime rewrite | Explicit non-scope |
| Spawn tests fail without session-worker binary | Mandatory build precondition in acceptance commands |

## Acceptance checks / tests

### Must pass (Implement / Review / Verify)

1. **Late Attach after PeerClosed** — PeerClosed first; enqueue Attach with grant; OperatorError (`local_webrtc_peer_gone` or equivalent); no residual attach; live attach counter not increased.
2. **Late Spawn (or equivalent durable non-Attach Request) after PeerClosed** — production-handler path; OperatorError peer-gone; **no new session / durable spawn ownership** created. Proves universal Request gate is not Attach-only.
3. **Late Subscribe after PeerClosed** — existing test remains green.
4. **Late Unsubscribe does not delete replacement owner (seq 5)** — Peer A subscribed with id S then closed (or A unsubscribed and B took S while A is not live); grant A not live; send `UnsubscribeEntities` for S with `grant_id=A`. **Must preserve** B's row, `owner_grant_id=B`, and entity counters. Red-on-revert: restore blind remove → B's row deleted.
5. **Attach residual owner sweep** — Attach succeeds while peer live; PeerClosed with empty attach snapshot still detaches grant-owned attach.
6. **Bounded close with production-handler deadline**
   - Deterministic close-hang on production `remove_peer` / PeerClosed path.
   - Run handler on dedicated thread with bounded join.
   - Named constants (e.g. `PEER_CLOSE_BOUND = 3s`, `HANDLER_JOIN_DEADLINE = 5s`).
   - Assert order: (1) handler returns within `HANDLER_JOIN_DEADLINE`; (2) fail-closed postconditions (map empty, runtime park, worker join, sibling cleanup).
   - Red-on-revert: unbounded `block_on` → handler join deadline fails.
7. **Sibling policy** — success: sibling survives; fail-closed/timeout: siblings cleaned.
8. **#200 regression suite** — map empty, park, close completion, fail-closed Err, reused entity id on PeerClosed, subscribe-first, session worker cleanup stay green.
9. **Red-on-revert** — late Attach, late Unsubscribe-reuse, hang-handler deadline each shown red when their fix is removed.
10. **Commands**
    ```sh
    cargo build --locked -p botster-core --bin botster-session-worker
    ./test.sh local_webrtc
    ```
    `./test.sh` only (sets `BOTSTER_ENV=test`). Spawn-bearing tests without the worker build are invalid evidence.

### Verify extras

- Production path evidence narrative: terminal signal → handler → forget → idle (not terminal JSON alone).
- Optional live no-spin CPU/sample path **or** registered dependency/waiver with residual risk named.
- Downstream overlays: [[botster-runtime-reviewer-playbook]], [[botster-runtime-verifier-playbook]].

## Vault gaps worth capturing

1. **Late Attach vs late Subscribe asymmetry after #200** — grant-tag + fail-closed + owner-sweep for all ownership-creating control (or fold into [[late webrtc messages after disconnect must not recreate clients]]).
2. **Late Unsubscribe must be owner-checked** — peer-gone path that removes by id alone can delete a live replacement owner after subscription-id reuse (bug on main after #200).
3. **Unbounded `block_on(peer.close())` hang** is distinct from close `Err`; bound + handler-return proof required.
4. No capture for rejected per-peer runtime rewrite.

Capture only after Implement confirms durable lessons.

## Product decision ledger (brief)

| Item | Decision |
| --- | --- |
| Architecture | Keep shared dedicated multi-peer runtime from #200 |
| Close hang | Timeout → fail-closed (sibling sacrifice named); handler must return by named test deadline |
| Late Attach | Grant-tag + reject + owner sweep |
| All tagged Requests | **Mandatory** peer-gone fail-closed when `grant_id: Some` |
| Late Unsubscribe | **Mandatory** owner-check; never delete replacement owner's row |
| Live no-spin | Preferred in Verify; else dependency/waiver |
| Dual planner | No |
| Per-peer runtime rewrite | Out of scope by default |
| Session worker build | Required before spawn-bearing tests |

## Pipeline gates and artifacts

- Plan gate `botster_stack_plan_gate`: this document + **structured** evidence fields (including discrete teardown lens fields).
- Next step: Plan Review (`botster_stack_plan_review`).
- Artifact path: `docs/plans/webrtc-teardown-follow-up-late-attach-bounded-close.md`.
