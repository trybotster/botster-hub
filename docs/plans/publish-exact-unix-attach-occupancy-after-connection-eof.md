# Plan: Publish exact Unix attach occupancy after connection EOF

Ticket: `ticket_1786870433_515008`
Run: `run_1786870436_668945`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)

Required by TUI ticket `ticket_1786868597_171437` / Plan Review `finding_1786870298_283386`.
TUI `ghostty-shared` must prove that Unix connection loss released the exact old `(session_id, subscription_id)` on a caller-owned Hub. Consumed `botster-hub-client` at Hub `4f30d695` / current main `60b79b8` has no public identity oracle for that fact.

Plan **revision 3** after Plan Review `review_1786871981_163553`.

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786871501_602222` late-message identity | product / high | Public `DaemonRequest::Detach` is pair-only. EOF cleanup is generation-scoped. Spawn is host-durable. **Resolved** in rev 2; late-request race model was still wrong. |
| `finding_1786871501_390930` duplicate checklists | process / info | Canonical Plan checklist is `checklist_1786870831_946262`. Duplicates skipped. **Resolved.** |
| `finding_1786871501_518123` worker-before-tests | process / low | Locked worker build before `./test.sh`. **Resolved.** |

## Plan Review corrections (rev 2 → rev 3)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786871981_654949` real Unix socket order | product / high | Ordinary `ControlMessage::Request` is request/response. The socket task waits in `receive_control_response`, writes the reply, then reads the next frame or EOF. `SubscribeEntities` also waits before its EOF loop. Committed ordinary requests therefore finish before this connection can observe EOF. Remove retired-client checks and the late Attach / queued Spawn / queued ShutdownSession tests. The only fire-and-forget control send is `RegisterUnixAdmission`. Fix that with an owner oneshot acknowledgement before the request loop, matching `Request`. |
| `finding_1786871981_560793` unbounded tombstone | product / medium | Do **not** keep a retired-`client_id` set. Owner acknowledgement makes Register-before-cleanup structural. No new unbounded map. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` from `list_spawn_targets` (`~/Projects/botster-hub`) |
| Plan worktree | this pipeline worktree |
| Worktree hygiene | tracked `.gitignore` has 53 bytes and matches `HEAD`; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `60b79b814df0af234c8b4d6429b6c577b52c6dd6` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** — not a consumer of Hub session-type eligibility work |
| `teardown_class_applies` | **yes** — Unix connection teardown, multi-client attach ownership on one session, Core generation release, and host-session-alive vs client-dead divergence |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Routing did not use the process working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] — planner Must Load. Ownership comes from the Hub charter, not this mixed-generation index.
- [[spa-patterns]] — planner Must Load only. This ticket has no React/SPA edit surface.
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[cross repo dependency registration must use dependency repo target]]
- [[prefer framework and library components over custom solutions]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Repository overlay for public DTO work inside this repo:

- [[botster-hub-client-playbook]]

Runtime-teardown class applies. Loaded:

- [[botster runtime teardown lenses]]
- [[botster-runtime-reviewer-playbook]]
- [[botster-runtime-verifier-playbook]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- other repository charters (`botster-core`, `botster-tui`, `botster-web`, `botster-tui-kit`, `botster-terminal-ghostty`, `botster-workspaces`) — this run stays on `botster-hub`; TUI consumption is a registered dependency on the TUI target, not a reason to load or edit those charters here

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub client crate is the external client boundary]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[daemon socket attach must detach subscriptions on disconnect and exit]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[Hub route registry names describe ownership not attach queues]]
- [[attach failed cleanup is route aware and idempotent]]
- [[Unix mux host events are unsolicited control frames]]
- [[first-party Unix attach clients use split Hello and subscription close events]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[a regression test must be shown to go red with the fix reverted]]
- Plan Review `review_1786871501_996838` / `finding_1786871501_602222`, `finding_1786871501_390930`, `finding_1786871501_518123`
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[hub generated protocol changes are a four site release chain]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]

## Context loaded

- Ticket `ticket_1786870433_515008` on Hub target `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Parent north-star Hub ticket `ticket_1786661010_115885` / run `run_1786867245_870799` registered TUI ticket `ticket_1786868597_171437` as a blocker. TUI Plan Review `finding_1786870298_283386` rejected local-map retirement, sibling send-and-echo, and a replacement Attach as release proof.
- TUI now depends on this ticket via `dependency_1786870438_296010`. This Hub ticket has no outbound dependencies.
- Current public Status exposes `lifecycle_counters.live_attach_subscriptions` as a counter only (`crates/botster-hub-client/src/lib.rs`).
- `DaemonEvent::TerminalSubscriptionClosed` is emitted from `unix_terminal_adapter.rs` after adapter close while the mux stays readable. It is not emitted on connection death, Detach, process exit, or session removal. A dead Unix socket cannot receive that event anyway.
- `list_terminal_subscriptions()` and `live_attach_routes` are Hub-internal. TUI must not import them.
- Unix production cleanup is `handle_connection` → `ConnectionCleanupGuard` drop → `handle_connection_cleanup` in `src/daemon_transport.rs`. That path currently:
  - `take_connection_bound_routes` then `live_attach_routes.remove` directly
  - independently `saturating_sub` `live_attach_subscriptions` for `cleanup.attached_subscriptions`
  - for bound routes, `close_adapter` + `cancel_stream` without going through `record_attached_subscription_change(Detach)`
  - skips a replacement owner via `connection_bound_route_still_owned` / `stream_owner_client_id`
- Attach/Detach occupancy is supposed to go through `record_attached_subscription_change`, which treats `live_attach_routes` as the occupancy set. PeerClosed already follows that rule. Unix EOF does not.
- Core already exposes `HubRuntime::list_terminal_subscriptions` and `HubRuntime::detach_terminal_subscription(session, subscription, generation)`. No Core ticket.
- Protocol is 7 / conformance 42. `@trybotster/hub-test-support` source is `0.1.37`. Default required features do not include additive adapter/close tokens.
- Existing `unix_adapter_stale_disconnect_does_not_cancel_replacement_owner` proves replacement-owner survival with `cleanup_completed` plus mux echo. That is not exact-pair occupancy proof.
- Existing `client_eof_detaches_connection_subscriptions` only proves the connection thread enqueues `Detach` on a mocked control channel. It does not prove Hub occupancy or Core generation release.

## Scope and non-scope

### Scope

1. **Public occupancy oracle on the client protocol.** Add a named occupancy projection that a sibling Unix client can read with only `botster-hub-client`:
   - `DaemonAttachOccupancy { session_id, subscription_id, generation }`
   - `DaemonStatus.live_attach_occupancy: Vec<DaemonAttachOccupancy>` with `serde(default, skip_serializing_if = "Vec::is_empty")`
   - Presence rule: a pair is listed if it is in Hub `live_attach_routes` **or** still present in Core `list_terminal_subscriptions()`. Absence means both layers are clean.
   - Advertise optional `FEATURE_ATTACH_OCCUPANCY` (`attach_occupancy`) on `DaemonCompatibility::current()`. Do **not** add it to `DaemonCompatibilityRequirement::current()`. Empty occupancy on an old daemon that omits the field must not be treated as proof; the feature token is the fail-closed signal.
2. **Unix connection death releases that exact occupancy and Core generation.** Change `handle_connection_cleanup` so each still-owned `(client_id, session_id, subscription_id, generation)`:
   - re-resolves generation at cleanup time with `live_generation_for_route(this client_id, session, sub)`
   - if that lookup is `None`, skip (replacement or sibling owns it, or it is already gone)
   - calls `detach_terminal_subscription` for **that** generation only
   - closes the bound adapter and cancels the Hub stream
   - releases occupancy through `record_attached_subscription_change(Detach)`
   - never calls public pair-only `DaemonRequest::Detach` from EOF cleanup
   - never `ShutdownSession`
   - never independently `live_attach_routes.remove` + `saturating_sub`
3. **`RegisterUnixAdmission` owner acknowledgement.** Today this send has no reply. Add a oneshot ack. The owner inserts the admission, then acks. The socket task waits for that ack before the request loop. Cleanup `Drop` cannot run until the task returns, so a completed Hello always has its admission inserted before EOF cleanup. No retired-`client_id` tombstone.
4. **Separate oracles.** Occupancy cleanup and replacement-owner protection stay separate tests with separate ablations and expected failure locations.
5. **Compatibility cutover inside this repo.** Keep `PROTOCOL_VERSION = 7`. Bump `CONFORMANCE_FIXTURE_REVISION` 42 → 43. Update generated TypeScript, first-party support matrix, `docs/client-protocol.md`, and hub-test-support source. If `0.1.37` is already on npm, cut source over to unpublished `0.1.38` and regenerate metadata checksums.
6. **Hub-owned production proof** using IsolatedHub + two real Unix `botster-hub-client` connections. Record Hub SHA and lockfile Core SHA plus binary realpaths.

### Non-scope

- Do not edit `botster-tui` or `botster-web`.
- Do not emit terminal bodies. Do not restore Hub Drain translation. Do not decode `READY` / `PAGE` / `FINISH` / Snapshot.
- Do not emit `TerminalSubscriptionClosed` on the dead socket as the occupancy oracle. Sibling `Status` is the oracle. Keep existing adapter-close event emission on a still-readable mux.
- Do not change WebRTC PeerClosed except to avoid regressing its existing occupancy path.
- Do not `ShutdownSession` on client EOF.
- Do not raise the default client feature floor.
- Do not bump protocol version.
- Do not add a second request type unless Status cannot carry the named pairs. Status is the ticket's named sibling query.
- Do not publish `@trybotster/hub-test-support` in this ticket. TUI Git-consumes `botster-hub-client`. Site-3 npm publish is a follow-up Hub ticket only if a Web consumer needs the new Status field. Do not register a Web ticket for this Unix occupancy work.
- Do not create a Core ticket. The exact-generation detach and inventory APIs already exist.
- Do not create a PR.
- Do not add a retired-`client_id` tombstone or fail-closed checks for ordinary requests after EOF. That order cannot happen on the production socket.
- Do not invent late Attach / Subscribe / Spawn / ShutdownSession races on one Unix connection.

## Repository ownership boundaries and cross-repo dependencies

| Owner | What |
| --- | --- |
| Hub (`tgt_7e208a0c76a44980a83b63af976b1f22`) | Unix EOF cleanup policy, `live_attach_routes` occupancy, Status projection, `RegisterUnixAdmission` ack, IsolatedHub proof |
| `botster-hub-client` (crate in this repo) | Public DTOs, feature constant, conformance revision, generated TypeScript |
| Core (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) | Terminal subscription identity `(session, subscription, generation)`, `detach_terminal_subscription`, inventory. **No change.** |
| TUI (`tgt_c3d470bab78549df920a41e8fb0e58d8`) | Consumes the public oracle after this merge. Already registered as depending on this ticket. Do not edit TUI here. |
| Web (`tgt_40abcf71ccf049f4ac0c99953a799869`) | Out of scope. Do not vendor or pin from this run. |

No new cross-repo dependency to register. A Core ticket would be wrong-repo routing. A TUI edit in this worktree would be wrong-repo implementation.

## Assumptions and unknowns

- Assumption: sibling `DaemonRequest::Status` is the public query. Ticket text says "Status/query". A new `ListAttachOccupancy` request would be extra surface without a consumer need.
- Assumption: including `generation` on the public row is allowed. TUI lookup key is the pair; generation lets the replacement-owner oracle stay public without Hub internals. Occupancy-cleanup absence is still pair absence.
- Assumption: union of Hub routes and Core inventory is the only way an ablation that leaves the pair in either layer fails at the public exact-absence assertion.
- Assumption: `0.1.37` is published or must be treated as immutable. Implement verifies npm; if published, source becomes `0.1.38`.
- Unknown: exact TUI `DaemonStatus` literal compile cost. Implement measures with a scratch Cargo patch worktree, then records the failures. That probe is not a TUI edit.
- Unknown: whether any in-workspace `DaemonStatus { ... }` literals need `#[non_exhaustive]`. Prefer adding the field with serde defaults over making the whole Status non-exhaustive unless the scratch probe shows a broader break. Workspace literals are updated in this repo.
- Not unknown: TerminalSubscriptionClosed-on-EOF is not the oracle. Do not ask a human for that.
- Not unknown: ordinary requests cannot run after this socket's EOF cleanup. The RegisterUnixAdmission ack is the smallest bound. No tombstone.

## Affected surfaces/files

| Surface | Change |
| --- | --- |
| `crates/botster-hub-client/src/lib.rs` | `FEATURE_ATTACH_OCCUPANCY`, `DaemonAttachOccupancy`, `DaemonStatus.live_attach_occupancy`, conformance 43, advertised feature list, compatibility tests |
| `crates/botster-hub-client/src/typescript.rs` and `generated/daemon-protocol.ts` | optional occupancy field |
| `src/daemon_projection.rs` | project occupancy onto Status |
| `src/daemon_transport.rs` | `handle_connection_cleanup` uses live generation for this `client_id` + `record_attached_subscription_change`; `RegisterUnixAdmission` oneshot ack before the request loop; Status unions Hub routes with Core inventory |
| `src/daemon_attach_stream.rs` | reuse existing generation / still-owned helpers; no new ownership ledger |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | sibling occupancy-after-EOF test + two targeted ablations; replacement-owner as a separate oracle; Spawn-then-EOF |
| `src/daemon_transport.rs` unit tests | occupancy set vs independent counter; second Detach/EOF is idempotent; RegisterUnixAdmission ack-before-request-loop |
| `docs/client-protocol.md` | document occupancy field, feature token, pair-absence meaning, generation as identity not TUI lookup |
| `packages/hub-test-support/*` | matrix + generated protocol + metadata checksums; unpublished version if `0.1.37` is published |
| `crates/botster-hub-client/src/lib.rs` tests / `hub_client_api_test` | default requirement still accepts previous descriptor; occupancy-specific requirement rejects it |

Do not touch `botster-tui` or `botster-web` trees.

## Risks

- Publishing only the counter or only Hub `live_attach_routes` lets a Core leak pass the public assertion.
- Treating sibling echo or a new Attach as occupancy proof. Those remain non-oracles.
- Independent `saturating_sub` after `live_attach_routes.remove` desynchronizes the counter the same way the forbidden PeerClosed path did.
- Closing a bound adapter without `detach_terminal_subscription` leaves the Core generation live.
- A delayed EOF after subscription-id reuse detaches B if cleanup uses pair-only `DaemonRequest::Detach` from the connection-thread snapshot.
- Leaving `RegisterUnixAdmission` fire-and-forget lets cleanup run before the owner inserts the admission.
- Adding occupancy to the default required feature list breaks old-hub Hello for unchanged clients.
- Empty occupancy without the feature token looks like success on an old daemon.
- Changing hub-test-support bytes under a published `0.1.37` violates immutability.
- Scratch-patch TUI probe contaminates the primary TUI checkout if it is not isolated.
- Emitting TSC on EOF or restoring Drain translation to "help" the oracle.

## Runtime-teardown lens answers

| Field | Answer |
| --- | --- |
| `teardown_class_applies` | yes — Unix socket death, multi-client attach ownership, Core generation release, host session stays running |
| `teardown_isolation` | One Unix connection's still-owned `(session_id, subscription_id, generation)` rows die. Sibling attach on the same session stays owned. Host session stays running. Entity subscriptions on that connection are already released today; keep that isolation. |
| `teardown_bounds` | Cleanup runs on the daemon control thread. `detach_terminal_subscription` is the existing synchronous Core host-tick API. Adapter `close` is non-blocking. Do not add `block_on` of a hanging library close. Mux `close_all` for that client remains bounded by existing Unix adapter close. If Core detach returns a typed miss, treat it as already-gone (idempotent), not as hang. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | IsolatedHub Unix: Hello → **wait for `RegisterUnixAdmission` ack** → request loop. Each ordinary request is `send Request` → `receive_control_response` → write reply → then read next frame or EOF. `SubscribeEntities` waits for its reply before its EOF loop. Socket EOF → `ConnectionCleanupGuard` drop → `handle_connection_cleanup` → `live_generation_for_route(this client)` → `detach_terminal_subscription` → `record_attached_subscription_change(Detach)` → sibling Status omits the old pair. Host session stays listed. No `ShutdownSession`. Ablations redden at exact-absence. Register-ack proof and replacement-owner proof are separate. Record Hub SHA `60b79b8` lineage plus locked Core `fc541a59` binary realpaths. |
| `ownership_identity` | Core owner is `(session_id, subscription_id, generation)`. Hub occupancy key is the pair. Connection owner is `client_id`. Public Detach identity is the pair only. EOF identity is the generation still owned by this `client_id` at cleanup time. Reused subscription ids after explicit Detach are a new generation. |
| `sibling_fail_closed_policy` | On successful EOF cleanup: sibling attach and host session keep working. On cleanup failure: increment `cleanup_failed`, do not `ShutdownSession`, do not detach the sibling, do not invent occupancy absence. Ultimate Core detach error for this generation fails closed for that row only. |

### Production Unix socket order (rev 3)

`handle_connection_async` after Hello:

1. Send `RegisterUnixAdmission` (**today: no ack**). This is the only fire-and-forget owner insert on this path.
2. Request loop: read one `DaemonRequest`, send `ControlMessage::Request`, **wait** `receive_control_response`, write the reply, then loop. EOF is observed only on the next `read_async_frame`.
3. `SubscribeEntities` leaves this loop via `handle_entity_subscription_async`, which also waits for the subscribe reply before it reads EOF.

Therefore a committed ordinary request on this connection **cannot** still be queued when this connection's cleanup runs. The socket task is still inside `receive_control_response` until the owner finishes that request. Cleanup `Drop` runs only after the task returns.

`cleanup_rx` before `control_rx` still matters for **`RegisterUnixAdmission`**, because that send does not wait. Hello can complete, the task can die, cleanup can run, and then the owner can insert the admission. Rev 3 fixes that by waiting for an owner oneshot after insert, before the request loop. After the ack, admission is present. Cleanup then always removes a row that exists. No tombstone. No unbounded set.

Replacement-owner is a **different connection**. B's Attach is not a late message on A's socket. Protection remains live generation lookup at A's cleanup time.

### In-flight and residual policy

| Class | Policy |
| --- | --- |
| Ordinary request on this socket (`Attach`, `Detach`, `Spawn`, `SubscribeEntities`, `ShutdownSession`, package writes, …) | Completes on the owner thread before this socket can observe EOF. Keep the committed host result. Cleanup does not roll it back. |
| EOF after a completed `Spawn` | Host session stays. Cleanup does not `ShutdownSession`. |
| EOF after a completed `Attach` | Release this client's live generation only. |
| `RegisterUnixAdmission` | Wait for owner ack after insert, before the request loop. Ablating the wait is the registration-order proof. |
| EOF cleanup vs replacement owner | Re-resolve `live_generation_for_route(this client_id, session, sub)`. `None` means skip. Never pair-only Detach from the stale snapshot. |
| Impossible on this socket | Late Attach / Subscribe / Spawn / ShutdownSession after this connection's cleanup. Do not implement or test those. |

### Late-message matrix

| Message | Real identity | After this connection EOFs | Residual sweep |
| --- | --- | --- | --- |
| `RegisterUnixAdmission` | `client_id` | Cannot race cleanup once the socket waits for the insert ack. Admission is present, then cleanup removes it. | Admission row removed on cleanup. |
| `Attach` | `client_id` + pair. Core assigns generation. | Already completed or never sent. Sibling Attach on another socket is a new owner. | EOF releases only a generation still owned by this `client_id`. |
| Explicit `Detach` | **pair only** | Already completed or never sent. Sibling may Detach its own pair. EOF cleanup must not send this request. | Occupancy change after a successful explicit Detach. Mux suppress is not ownership. |
| EOF cleanup | `client_id` + pair + **live generation for that client** | `detach_terminal_subscription` for that generation. Skip when live lookup is `None`. | `record_attached_subscription_change(Detach)` after that generation is gone. |
| `SubscribeEntities` / `UnsubscribeEntities` | connection-scoped `subscription_id` | Subscribe reply happens before EOF can be read. Cleanup releases remaining ids. | Entity-subscription release. Not the occupancy oracle. |
| `Spawn` / `SpawnSessionType` | host `session_id` | Already completed before EOF. Host session survives. | Session remains in `ListSessions`. |
| `ShutdownSession` / `RemoveSession` | host `session_id` | EOF must not issue these. An explicit request already completed if it was sent. | Occupancy-after-EOF tests send neither. |
| Session-type / spawn-target / worktree / package create-update-delete | host registry identity | Already completed if sent. Survive EOF. | Out of occupancy scope. |
| `StartHubUpdate` | host update execution | Already completed if sent (`StartHubUpdate` also waits for delivery ack). Survive EOF. | Out of occupancy scope. |
| `Drain` / `SendInput` / `Resize` / `ModeGatedInput` | route owner | Already completed or never sent. Sibling may continue. Must not create occupancy. | `drain_does_not_change_attach_occupancy` stays true. |
| `Status` / `ListSessions` / other reads | none | Sibling Status is the occupancy oracle. | N/A |
| Hello / new connection | new `client_id` | New connection is not this cleanup. | N/A |
| `TerminalSubscriptionClosed` | adapter-close event on a live mux | Not sent on the dead socket. Not the occupancy oracle. | Unchanged adapter-close emission for still-readable connections. |
| WebRTC `LocalWebrtcPeerClosed` | `grant_id` | Out of scope except no occupancy-path regression. | Existing PeerClosed occupancy path. |

## Acceptance checks/tests

Implement must prove the production path, not merely that types exist.

### Occupancy cleanup (primary oracle)

IsolatedHub, two Unix clients, **same session, different `subscription_id`s**:

1. A attaches `(session, sub-a)`. B attaches `(session, sub-b)`.
2. Sibling Status (B, `botster-hub-client` only) lists both pairs. `compatibility.features` contains `attach_occupancy`.
3. A socket EOF / shutdown. Do not send Detach from A as the proof path. Do not `ShutdownSession`.
4. B Status: `(session, sub-a)` is absent. `(session, sub-b)` is present. This is the exact-absence assertion.
5. B `SendInput` is accepted. Host `ListSessions` still contains `session`.
6. No test may treat sibling echo or a new Attach as the release proof.

### Ablations (must redden at exact-absence)

| Ablation | Expected first failure |
| --- | --- |
| Leave `(session, sub-a)` in `live_attach_routes` after EOF | B Status still lists the pair. Fail at exact-absence. Must not reach sibling-echo. |
| Skip `detach_terminal_subscription` so Core inventory still has the pair | Union still lists the pair. Fail at exact-absence. Must not reach sibling-echo. |
| Independent `saturating_sub` that drops the counter but leaves the pair | Exact-absence still fails because the oracle is named pairs, not the counter. |

Each ablation is its own test or `#[cfg(test)]` hook. One red suite does not vouch for the other claim.

### Replacement-owner (separate oracle)

Keep and tighten `unix_adapter_stale_disconnect_does_not_cancel_replacement_owner`:

1. A attaches `(session, sub)`, **explicit pair-only Detach**, B attaches the same pair (new generation).
2. Delayed A EOF.
3. Public occupancy still lists `(session, sub)` with **B's generation**.
4. B scoped Drain stays bound. Mux echo is not this oracle.
5. Separate ablation: EOF cleanup that sends pair-only `DaemonRequest::Detach` from A's stale snapshot. Fail at "B's generation still occupied", not at occupancy-cleanup absence.

This race is not structural queue order. B is a different connection. Protection is live `live_generation_for_route(A, session, sub) == None` plus `connection_bound_route_still_owned(A, …, gen_a) == false`.

### Real races (not ordinary-request-after-EOF)

| Race | Why it is real | Pass | Ablation first failure |
| --- | --- | --- | --- |
| Stale pair-only Detach | B is a **different** connection. A's cleanup snapshot can still name the pair after B owns the generation. | Occupancy still B's generation | B generation gone |
| `RegisterUnixAdmission` without ack | Fire-and-forget send. Cleanup can run before the owner inserts. | After Hello, owner has the admission before the request loop. Cleanup removes that row. Ablate the wait. | Admission missing at first request, or cleanup-then-insert leaves a stale admission |

Also prove: A `Spawn`s, receives the Spawned reply, then EOFs. Host `ListSessions` still contains the session. EOF sent no `ShutdownSession`. Do **not** test queued Spawn / Attach / ShutdownSession after this socket's cleanup.

### Compatibility and workspace

- Default `DaemonCompatibilityRequirement::current()` accepts the previous descriptor (no `attach_occupancy` required).
- Occupancy-specific requirement rejects the previous descriptor and accepts the current one.
- Support matrix lists `attach_occupancy` under `supported_features` only.
- `PROTOCOL_VERSION` stays 7. Conformance becomes 43.
- **First:** `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
- **Then:** `./test.sh` workspace, IsolatedHub occupancy and race tests, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`.
- `packages/hub-test-support` `npm run check` after asset sync.
- Scratch Cargo-patch TUI worktree: `cargo check --workspace` and `--all-targets` against the candidate `botster-hub-client`. Record compile cost. Delete the scratch worktree. Do not commit TUI changes.
- Live proof records Hub checkout SHA and lockfile Core SHA plus both binary realpaths.

### Downstream consumer

TUI ticket `ticket_1786868597_171437` consumes this after merge. This run's downstream proof is:

- the public Status field is reachable from `botster-hub-client` only
- scratch-patch compile evidence for TUI DTO literals
- no TUI source change

Do not run `script/test-live-hub ghostty-shared` in this Hub worktree.

## Vault gaps worth capturing

After Implement, capture if still true:

1. Unix EOF occupancy must share `record_attached_subscription_change` / `live_attach_routes` with Attach, Detach, and PeerClosed. Today's `handle_connection_cleanup` is the Unix instance of the PeerClosed independent-counter gotcha.
2. A public occupancy oracle must union Hub routes with Core inventory, or pair-absence on Status can pass while Core still owns the generation.
3. `live_attach_subscriptions` and sibling echo are not identity oracles. The feature token is required so an omitted field cannot be read as absence.
4. Ordinary Unix requests are request/response. Only `RegisterUnixAdmission` needs an owner ack. A retired-id tombstone is the wrong bound.

No Plan-time inbox capture.

Vault checklists for this Plan visit:

- Canonical: `checklist_1786870831_946262` (standard vault items, all done).
- Duplicate timeout retries, items skipped: `checklist_1786870844_969375`, `checklist_1786870864_647394`.
- Plan Review later created run-scoped `checklist_1786871224_420878`. That is Review's checklist, not a second Plan checklist.
- This visit does **not** create another Plan checklist.

## Implement sequence

1. Restore `.gitignore` from `HEAD` if a later step wipes it. Keep colon-free `CARGO_TARGET_DIR` unused (path has no `:`).
2. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` before any `./test.sh` or IsolatedHub test.
3. Add DTO + feature + conformance 43. Update workspace Status literals.
4. Project occupancy as the Hub∪Core set on Status.
5. Give `RegisterUnixAdmission` a oneshot ack. Owner inserts, then acks. Socket waits before the request loop. No retired-id set.
6. Rewrite Unix `handle_connection_cleanup` onto live `live_generation_for_route(this client)` + `detach_terminal_subscription`. Do not call pair-only `DaemonRequest::Detach`.
7. Add IsolatedHub occupancy-after-EOF test, two occupancy ablations, replacement-owner pair-only-Detach ablation, Register-ack order proof, and Spawn-then-EOF session-survives proof. Do not add late ordinary-request tests.
8. Sync generated TS and hub-test-support assets. Bump unpublished package version if `0.1.37` is published.
9. Re-run locked worker build, then `./test.sh`, fmt, clippy, npm check.
10. Scratch-patch TUI DTO probe. Record evidence. Remove the scratch worktree.
11. Merge directly to `main`. Do not open a PR.
