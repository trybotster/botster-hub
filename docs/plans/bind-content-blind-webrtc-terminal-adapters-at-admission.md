# Plan: Bind content-blind WebRTC terminal adapters at admission

Ticket: `ticket_1786661008_247079`
Run: `run_1786704125_619383`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Plan **revision 2** after Plan Review `review_1786705428_362741`

## Plan Review corrections (rev 2)

| Finding | Status |
| --- | --- |
| `finding_1786705428_174569` DataChannel close has no verified bound | **Locked.** `close_data_channel` today awaits `local_close()` with no timeout. Wrap that future in `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Timeout or error still calls `cleanup_once`. Do not retry a hung DataChannel close. `OnClose` / `OnError` already fail into this function. The control-plane hard stop remains `LocalWebrtcPeerClosed` → `remove_peer` → bounded `PeerConnection::close` → runtime park/drop. Add a production-handler hang inject that proves adapter `Closed` and peer retirement when `local_close` never returns. |
| `finding_1786705428_612131` delivery-kind change lacks downstream consumer proof | **Locked.** Current Web (`tgt_40abcf71ccf049f4ac0c99953a799869`) pins `@trybotster/hub-test-support@0.1.32` and rejects any delivery kind other than `daemon_response` / `daemon_entity_frame` in `webrtcDaemonClient.ts`. This ticket does not implement the Web decoder and does not bump that pin. Required proof: (1) regenerate in-repo TypeScript and hub-test-support 0.1.35; (2) live current packaged Web / existing unbound WebRTC attach against this Hub without Hello still Drains and emits no `daemon_terminal_frame`; (3) record the consumer pin. Sites 3–4 of [[hub generated protocol changes are a four site release chain]] stay with publish + the Web ticket. |
| `finding_1786705428_726530` affected-file list omits test wiring | **Locked.** New tests are `include!`d from `tests/hub_daemon_lifecycle_test.rs` (same as Unix). Update exhaustive `DaemonLocalWebrtcDeliveryKind` matches in `webrtc_fixtures.rs`, `src/local_webrtc.rs`, and `src/local_webrtc_smoke.rs`. `mod.rs` already exports `webrtc_fixtures`; do not add a second module path. |
| `finding_1786705428_441823` clean-base test omits worker build | **Locked.** README sequence is worker build, then fmt/clippy, then `./test.sh --locked`. Plan Review reproduced four `No such file or directory` worker failures without that build. |

Published npm `@trybotster/hub-test-support` is `0.1.33`. This tree is unpublished `0.1.34`. Fixture or protocol-byte changes must use unpublished `0.1.35`.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` (`trybotster/botster-hub`) |
| Plan worktree | this pipeline worktree; Plan does not mutate `Cargo.lock` |
| Worktree hygiene | tracked `.gitignore` has content; path has no `:`; no `CARGO_TARGET_DIR` override required |
| Merge policy | direct into `main`; do not create a PR |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. The ambient checkout is the same repository. Routing did not infer the repository from the working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] (index only; ownership comes from the Hub charter)

Repository overlay implicated by public feature/DTO work inside this repo:

- [[botster-hub-client-playbook]]

Runtime-teardown class applies. Loaded:

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[spa-patterns]] — no React/SPA implementation in this ticket
- other repository charters — this run stays on `botster-hub`

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Core bind stores an immutable negotiated terminal capability set]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Core ClientWorker bind requires a live attach generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Hub route registry names describe ownership not attach queues]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[file descriptor exhaustion from stale webrtc connections]]
- [[late webrtc messages after disconnect must not recreate clients]]
- [[webrtc bootstrap origin must be requested after the package server binds]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[adding a hub client feature constant is a three site change]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[pipeline artifacts should cite vault notes by wikilink not home path]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[plan steps need reviewable plan artifacts]]
- [[test script required for rust tests not cargo test]]
- [[transport ownership north star for modular Botster is proposed]]
- [[proposed Hub admission binds adapters with negotiated subscription capabilities]]
- [[proposed Core transport adapters use bounded writes without policy queues]]
- [[proposed Hub terminal tests enforce content blind adapters]]
- [[proposed dead sink handling triggers one Core detach without a Hub round trip]]
- [[proposed transport lifecycle lets control connections outlive terminal subscriptions]]
- [[proposed ProcessExited closes terminal subscriptions but not the host session]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster web generated protocol drift checks need explicit hub artifact paths]]
- [[hub generated protocol changes are a four site release chain]]

The north-star notes remain `decision_state: proposed` in the vault. This ticket is an authorized slice of project `project_1786660949_205223` and therefore implements those rules for the WebRTC DataChannel without treating the wider proposal as already ratified for cold-cut, Web, or TUI.

## Context loaded

- Pipeline ticket, run, closed Unix parent, project north star, and sibling tickets via `project_pipelines_current_context` / `project_pipelines_get_project` / `list_spawn_targets`
- Closed Hub parent `ticket_1786661008_634435` (same `target_id`): Unix adapter, one-slot mux, Hello opt-in, bound disconnect = adapter close only, one-frame Attaching exception, fail-closed pre-bind
- Closed Core parents on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`: adapter contract/harness, ClientWorker push, immutable capability-set bind
- Locked Core SHA already in this Hub lock: `f4f6bf5babe92dfb9241a760c414187f711c2c42`
- Current Hub client: `PROTOCOL_VERSION` 7, `CONFORMANCE_FIXTURE_REVISION` 39, optional `unix_terminal_adapter`, in-tree hub-test-support `0.1.34` (published npm is `0.1.33`)
- Plan Review `review_1786705428_362741`: four product findings (DataChannel close bound, downstream consumer proof, test wiring, worker build)
- Current Web spawn target `tgt_40abcf71ccf049f4ac0c99953a799869` pins `@trybotster/hub-test-support@0.1.32` and rejects unknown delivery kinds
- Production WebRTC path: `IssueLocalWebrtcBootstrap` after listener bind → `LocalWebrtcSignal` (grant, secret, origin, offer) → AES-GCM DataChannel → encrypted `DaemonRequest` → existing `frame_encrypted_daemon_delivery` for control and entity frames
- Current Attach handler binds Unix only when `grant_id` is absent and Hello requires `unix_terminal_adapter`. WebRTC attaches stay unbound and Drain
- Current `LocalWebrtcPeerClosed` always calls `detach_local_webrtc_subscriptions` (Hub Detach) after occupancy release
- DataChannel inbound decrypt accepts only `DaemonRequest`. There is no DataChannel Hello today

This ticket is **not** a consumer of Hub session-type eligibility work. No `list_session_types_for_target` pin injection.

## Botster layers touched

- Rust Hub daemon WebRTC transport, attach bind, and `HubRuntime` facade reuse
- In-repo `botster-hub-client` optional feature + terminal delivery kind
- Hub tests / live WebRTC proofs
- Not: Lua plugins, Web decoder, TUI, TUI Kit, Core implementation, Unix adapter rewrite, Project Pipelines package, cold-cut Drain removal

## Worktree / target assumptions

- Implement stays in this run's Hub worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Do not edit `botster-core`. The Core parents are closed and already locked.
- Do not `cargo update` unless `origin/main` moved forward while remaining a descendant of `f4f6bf5babe92dfb9241a760c414187f711c2c42`. Assert that lock SHA after any update.
- Reuse the shipped Unix bind sequence, capability intersection, fail-closed Attaching exception, and occupancy cleanup. Do not invent a second attach-phase machine.

## Scope

1. **Admit, then bind.** Create a WebRTC adapter only after all of these succeed:
   1. Pairing / trusted browser identity as today's grant issuance already requires.
   2. Origin-bound bootstrap after the package listener binds (`[[webrtc bootstrap origin must be requested after the package server binds]]`).
   3. `LocalWebrtcSignal` grant_id, grant_secret, origin, and offer checks.
   4. AES-GCM stream-key crypto on the DataChannel.
   5. Protocol admission: an encrypted DataChannel `DaemonHello` whose `required_features` include the new optional Hub feature `webrtc_terminal_adapter`.
   6. `Attach` returns exactly the initial `Attaching` sentinel, inventory has a live generation, then `bind_terminal_adapter`.
   Do not create an adapter at bootstrap, signal, or peer-map insert.

2. **Keep the one-frame Attaching exception.** WebRTC Attach uses the same Core attach path as Unix. The Attach response may translate only the initial `Attaching` frame. Any other pre-bind terminal event fail-closes: cancel the Hub route, close any adapter candidate, detach the live generation, return `attach_failed`, and require a fresh attach. Cold-cut `ticket_1786661010_198387` removes the exception. This ticket does not add a Core atomic attach-and-bind API.

3. **Optional feature. Do not always-bind.** Current Web and existing WebRTC proofs Drain. Always-bind would insert unsolicited terminal deliveries onto today's DataChannel and empty Drain Snapshot for current Web before `ticket_1786661008_897067`. Advertise `webrtc_terminal_adapter` in `DaemonCompatibility::current()`. Do not put it in `DaemonCompatibilityRequirement::current()`. Add `for_webrtc_terminal_adapter()`. `PROTOCOL_VERSION` stays 7. Bump `CONFORMANCE_FIXTURE_REVISION` 39 → 40 if fixtures or public types change. Bind only when the DataChannel Hello requires the feature and `grant_id` is present.

4. **Keep route records ownership-only.** Reuse `AttachStream` generation and adapter-bound flags. Store a crate-private bound-handle enum so the same route can hold a Unix handle or a WebRTC handle. Do not store READY / PAGE / FINISH / `Attached` / snapshot bytes. Prefer route and ownership names.

5. **Capability intersection at admission; bind the result into Core.** Reuse the shipped `f4f6bf5` API and the Unix token intersection (`negotiated_unix_capability_set` or a shared rename). Pass `TerminalCapabilitySet`. Include `snapshot_delivery=ready_then_history` only when Hello requires that Hub/terminal feature. Prove inventory echoes the same tokens.

6. **Content-blind write path.** `try_write` calls `TerminalFrame::to_bytes()` and stores those bytes in one slot. Hub must not deserialize frame bodies or branch on READY, PAGE, FINISH, later `AttachState`, or `GHOSTSNP`. Tests use opaque fixtures.

7. **One transport-internal active write. Framing, encryption, and chunking stay on the admitted DataChannel task.**
   - The adapter slot is transport state, not a ClientWorker policy queue. No adapter retry.
   - `try_write` / `close` / `Drop` stay non-blocking and lock-free for the writer. They must not `block_on` DataChannel I/O.
   - The existing DataChannel sender loop is the one writer. Do not add a second send task.
   - That loop encrypts the opaque slot with the grant stream key, chunks it with the existing delivery-chunk bounds, and sends those chunks as one delivery.
   - Add `DaemonLocalWebrtcDeliveryKind::DaemonTerminalFrame` so terminal deliveries are not fake `DaemonResponse`s. This preserves the Hub control plane.
   - One frame occupies the slot until every chunk of that delivery is sent or the adapter closes. `try_write` of the next frame returns `Full` until `complete_active`.
   - Chunks of frame N+1 must not start, interleave, or reuse the message_id of frame N.
   - Completing a write takes the slot once. Do not retransmit a completed frame.
   - Map DataChannel `buffered_amount` high/low onto the adapter `WouldBlock` flag. That is transport pressure, not a second queue.

8. **Propagate DataChannel close and grant revocation to adapter `Closed`.**
   - Bound WebRTC route + DataChannel close / peer_failed / peer_disconnected / grant `remove_peer` / fail-closed sibling grant drop: close every adapter owned by the removed grant_id. Do **not** send `HubClientRequest::Detach` or `detach_local_webrtc_subscriptions` for those bound routes. Core `Closed` is the one mechanical detach.
   - Bound `local_close` with `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Timeout or error still runs `cleanup_once`. That is the missing bound Plan Review found.
   - Authorized explicit client `Detach`: forward through `HubRuntime`, then close the leftover adapter.
   - Unbound WebRTC routes: keep today's Hub Detach (`local_webrtc_peer_close_detaches_terminal_subscriptions` stays valid).
   - Never call `ShutdownSession` from adapter close.
   - Late requests after PeerClosed continue to fail closed (`local_webrtc_peer_gone`). They must not recreate adapters or routes (`[[late webrtc messages after disconnect must not recreate clients]]`).

9. **PeerClosed occupancy and replacement owners stay separate proofs.**
   - Occupancy release uses `record_attached_subscription_change(Detach)` against `live_attach_routes`. Do not `saturating_sub` an independent counter (`[[PeerClosed attach occupancy must use the live attach route set]]`).
   - Replacement-owner proof uses a Hub-visible ownership oracle (bound Drain empty of terminal bodies, occupancy that still names B's generation, `bound_adapter_close` delta 0 for B). Mux or DataChannel delivery alone is not enough (`[[mux envelope delivery does not prove Hub route ownership]]`).
   - Connection-bound / grant-bound ledger stores session, subscription, and generation. Cleanup mutates a live route only when the closing grant still owns that generation.

10. **Keep control-plane drains.** Session lifecycle, entities, and unbound terminal routes continue to Drain. After bind, a bound WebRTC route must not emit AttachState, Snapshot, TerminalOutput, or ProcessExited via Drain or any later Attach response. The initial Attaching exception is Attach-response only, and only before bind.

11. **Import Core's harness.** The production WebRTC adapter (not a Core-shaped fake) implements `TerminalAdapterHarnessDriver` and passes `assert_terminal_adapter_conformance`. Same harness as Unix.

12. **Keep the separate Hub control plane.** Hello, signal, spawn, grants, entity frames, and Drain stay host-control. Terminal bodies leave only through the opaque adapter delivery kind.

## Non-scope

- Unix adapter rewrite (`ticket_1786661008_634435` is closed)
- Cold-cut of Hub terminal Drain translation and the one-frame Attaching exception (`ticket_1786661010_198387`)
- Web / TUI protocol-plane consumers and authentic Ghostty decoder proof
- Core contract, queues, attach phase machine, or harness ownership
- Dedicated WebRTC terminal DataChannels or a second peer
- Changing the shipped fail-closed dedicated-runtime blast radius
- Host session shutdown, worktree, or retention policy
- Raising the default client requirement or a protocol flag day
- Speculative write coordinators or a second send task
- Project Pipelines package/plugin work
- npm publish of hub-test-support (select unpublished `0.1.35` in-tree if fixtures change)

## Binding decisions

| Topic | Decision |
| --- | --- |
| When WebRTC Attach binds | Live grant, origin, crypto, and DataChannel Hello require `webrtc_terminal_adapter`; Attach returns only `Attaching`; inventory has a live generation |
| When WebRTC Attach stays unbound | No DataChannel Hello, or Hello omits the feature. Today's Web and existing Drain proofs stay on this path |
| One-frame exception | Same as Unix. Cold-cut removes it |
| WebRTC grant / revocation | Grant is `BootstrapGrant` + `grant_id`. Revocation of a live grant is `remove_peer` (DataChannel close, peer_failed, fail-closed sibling drop, or explicit forget). That closes adapters. It is not Unix connection death |
| Unix path | Unchanged. `grant_id` absent still uses Hello + `unix_terminal_adapter` |
| Write sink | The admitted DataChannel, muxed with encrypted control responses and entity frames |
| Delivery | New `DaemonTerminalFrame` kind + existing chunk bounds. Not a `DaemonResponse` |
| Why DataChannel Hello | WebRTC has no Hello today. Required features cannot live on Unix-socket `LocalWebrtcSignal` (package server, not browser). Always-bind would break current Web |
| Why not a second DataChannel | Ticket binds adapters at admission of the existing encrypted channel. Dedicated streams stay proposed |
| Core pin | Already `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `HubRuntime` | Reuse Unix facades. Do not add raw Core router calls |
| Threading | `try_write` / `close` on the owner tick. DataChannel task encrypts, chunks, and sends. No `block_on(close)` on the control thread |
| Bound-route peer death | Close adapter only. No Hub Detach. Prove one Core detach via inventory absence |
| Explicit Detach | Authorized request only. Forward, then close leftover adapter |
| Shared runtime fail-closed | Keep shipped policy: ultimate `PeerConnection::close` hang/failure drops the dedicated WebRTC runtime and sibling grants on that runtime. Name it. Do not silently widen or hide it |
| DataChannel `local_close` bound | Same `LOCAL_WEBRTC_PEER_CLOSE_BOUND` as `PeerConnection::close`. No retry on timeout. `cleanup_once` always follows |
| `PROTOCOL_VERSION` | 7 |
| Conformance / package | Revision 39 → 40 and hub-test-support 0.1.34 → 0.1.35 if public types or fixtures change |

## Runtime-teardown class answers

`teardown_class_applies`: yes. The ticket binds ClientWorker adapters on WebRTC peers, closes them on DataChannel close and grant revocation, and must not confuse adapter close with host session shutdown or treat a terminal JSON file as live peer teardown.

| Field | Answer |
| --- | --- |
| `teardown_isolation` | One WebRTC grant owns one peer, its DataChannel sender loop, and every bound adapter for that grant. Closing that grant tears down only that grant's `(session_id, subscription_id, generation)` set. Sibling grants on other peers stay live unless the shipped fail-closed dedicated-runtime path runs. Host session workers stay up. |
| `teardown_bounds` | Adapter `try_write` / `close` / `Drop` are non-blocking and must not `block_on` DataChannel I/O. **DataChannel path (this ticket):** `OnClose` and `OnError` already return into `close_data_channel`. That function currently awaits `data_channel.local_close()` with no timeout. Wrap `local_close()` in `tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, ...)`. On timeout or error, still call `cleanup_once(cause)`. Do not retry a hung DataChannel close. **Peer path (already shipped):** `remove_peer` → `close_peer_on_runtime` wraps `PeerConnection::close` in the same bound, retries once only on library error, and treats timeout as ultimate failure. **Hard stop:** `cleanup_once` → `LocalWebrtcPeerClosed` → `remove_peer` → close or fail-closed drop → park/drop dedicated runtime. A hanging adapter `close()` is an adapter defect and fails the Core harness. Hub must not add `block_on(adapter.close())` on the control thread. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | Exact happy path: package bind → `IssueLocalWebrtcBootstrap` → `LocalWebrtcSignal` (origin and secret) → DataChannel open → encrypted Hello with `webrtc_terminal_adapter` → Attach only `Attaching` → inventory bound + echoed capabilities → later frames as `DaemonTerminalFrame` chunks → `peer.close()` → `OnClose`/`OnError` → bounded `local_close` → `cleanup_once` → `LocalWebrtcPeerClosed` → `remove_peer` → adapter Closed, no Hub Detach, inventory gone, session listed, `has_live_peer` false, dedicated runtime parked when no peers remain. **Hang path:** inject a never-completing `local_close`; handler still reaches `cleanup_once` within `LOCAL_WEBRTC_PEER_CLOSE_BOUND`; adapter Closed; peer retired; control thread does not hang. **Live oracles required:** peer-map empty / `active_peer_count`, runtime parked or dropped, `bound_adapter_close` delta, `cleanup_hub_detach` delta 0, close-completion or handler-join within the bound. A `local-webrtc-sender-terminal.json` file alone is not proof (`[[terminal webrtc failure records do not prove peer runtime teardown]]`). Record Hub SHA, locked Core SHA, and both binary realpaths. |
| `ownership_identity` | Hub route key is `(grant_id, client_id=botster-hub-webrtc-{grant_id}, session_id, subscription_id, generation)`. Core identity is `(session_id, subscription_id, generation)` plus the bound capability set. Reused `subscription_id` is generation N+1. Delayed PeerClosed / late `Closed` for generation N must not delete N+1 or close B's adapter. |
| `sibling_fail_closed_policy` | Successful peer close: other grants keep working; host session stays. Bound adapters on the closed grant die together (`[[webrtc peer cleanup removes every per peer owner together]]`). Ultimate close failure: shipped dedicated-runtime drop also closes sibling grants that share that runtime. Test that blast radius. ProcessExited still does not shut down the host session. |

### Late-message matrix

| Message | Grant / owner tag | Reject after terminal failure | Residual sweep if it races close |
| --- | --- | --- | --- |
| DataChannel `Hello` | live `grant_id` after signal + crypto | Gone peer: no admission row, no adapter | Hello never creates a route |
| `Attach` | `grant_id` + `client_id=botster-hub-webrtc-{grant_id}` | Gone peer: `local_webrtc_peer_gone`. Bind only after Hello feature + Attaching-only. Any other pre-bind terminal event fail-closes | `cancel_stream` + generation detach; idempotent |
| `bind_terminal_adapter` | live attach generation + `TerminalCapabilitySet` | `BindBeforeAttach` / `UnknownSubscription` / `StaleGeneration` / `AlreadyBound`. Close the rejected adapter on the same stack | Rejected adapter never enters inventory |
| `Detach` | route owner `grant_id` | Foreign grant forbidden. Generation mismatch leaves N+1 | Forward Detach; close adapter if open |
| DataChannel `OnClose` / `OnError` | `grant_id` | No new Attach/bind/Hello for that dead grant | Enter `close_data_channel`. Bound `local_close` with `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Timeout or error still `cleanup_once`. Bound routes: adapter close only. Unbound: existing Hub Detach. Never `ShutdownSession` |
| Hung `local_close` | same `grant_id` | Same reject as peer gone after `cleanup_once` | Timeout cancels the future; `cleanup_once` still runs; `remove_peer` is the hard stop. No retry |
| DataChannel close / peer_failed / peer_disconnected | `grant_id` | No new Attach/bind/Hello for that dead grant | Same as `OnClose` / `OnError` after `cleanup_once` |
| Grant `remove_peer` / fail-closed sibling drop | removed `grant_id` set | Same as peer close for each removed grant | Close adapters owned by removed grants; occupancy via live route set |
| Late `Request` / `SubscribeEntities` / `UnsubscribeEntities` | existing grant tag | Gone peer already rejected; must not recreate adapter or entity row owned by another grant | Existing owner-checked sweep stays |
| `Drain` | existing `authorize_drain` | Foreign owner forbidden | Bound routes return control events only |
| `SendInput` / `Resize` / `ModeGatedInput` | existing HubRuntime session owner | After detach, Core rejects; Hub does not recreate the adapter | No adapter write of input |
| Inventory reconcile | control-plane only | Missing Core row → drop Hub route | Never infer from adapter silence |

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This ticket |
| --- | --- | --- |
| WebRTC admission (bootstrap, signal, origin, crypto, DataChannel Hello), route records, adapter instance, framing, encryption, chunked write | Hub | Implement |
| Terminal queues, attach phases, slow-client policy, mechanical detach, inventory, immutable bound capability set | Core | Consume shipped `f4f6bf5` API |
| `TerminalAdapter` + conformance harness | Core test support | Import; do not fork |
| Opaque `TerminalFrame` | `botster-terminal-protocol` | `to_bytes()` only; do not depend on the client crate |
| External control DTOs / feature constants / delivery kind | `botster-hub-client` (this repo) | Optional feature + `DaemonTerminalFrame` + Hello-on-DataChannel types only |
| Unix adapter | Hub parent ticket | Reuse; do not rewrite |
| Drain cold-cut | Hub sibling `ticket_1786661010_198387` | Leave translation in place |
| Web / TUI decoders | those targets | Downstream tickets; do not implement here |
| Current Web live consumer (`@trybotster/hub-test-support@0.1.32`) | `botster-web` `tgt_40abcf71ccf049f4ac0c99953a799869` | Read-only proof without Hello; do not edit that repo |

Registered dependencies:

- Hub Unix adapter `ticket_1786661008_634435` (closed, same target) — parent; reuse bind sequence and occupancy rules
- Core ClientWorker push `ticket_1786661004_845807` (closed, `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`)
- Core capability-bind `ticket_1786682902_405026` (closed, same Core target)

Sibling consumers to leave registered against their own targets:

- Hub cold-cut `ticket_1786661010_198387`
- Web `ticket_1786661008_897067`
- TUI `ticket_1786661009_551067`
- Integration `ticket_1786661010_115885`

Do not silently broaden this run onto those targets.

## Assumptions and unknowns

- Vault north-star notes are still marked proposed. The project ticket plus the closed Unix parent authorize this WebRTC slice.
- The one-frame Attaching exception remains a staged dual path until the cold-cut ticket.
- DataChannel Hello is the WebRTC analog of Unix Hello. It is protocol admission, not a second listen path.
- `DaemonLocalWebrtcDeliveryKind` is exhaustive today. Adding `DaemonTerminalFrame` is a Rust source-breaking enum addition. Wire stays compatible for old clients because they never Hello and never receive the new kind.
- Bound peer death is adapter close only. Idempotent Core detach is not a license for a second Hub Detach.
- Shipped dedicated-runtime fail-closed sibling sacrifice stays. This ticket names and tests it. It does not redesign WebRTC runtime topology.
- `docs/plans/` is the live Hub plan home (no retired-directory stub).

Unknowns Implement must resolve from code, not invent:

- Exact decrypt dispatch that distinguishes encrypted `DaemonHello` from `DaemonRequest` without treating Hello as `InvalidRequest`
- Whether HelloAck on the DataChannel is an encrypted `DaemonHelloAck` plaintext or a control `DaemonResponse`; pick the smaller existing DTO and document it
- Whether `AttachStream.adapter` becomes an enum in `daemon_attach_stream.rs` or a small crate-private handle trait. Prefer an enum over a new public abstraction
- Plan Review resolved the package version: published npm is `0.1.33`; this tree is unpublished `0.1.34`; protocol/fixture byte changes must use unpublished `0.1.35`

## Affected surfaces / files

Create:

- `src/webrtc_terminal_adapter.rs` — production one-slot adapter, per-peer mux, Core harness driver
- `docs/plans/bind-content-blind-webrtc-terminal-adapters-at-admission.md` — this plan
- `docs/reports/bind-content-blind-webrtc-terminal-adapters-at-admission-implement.md` — Implement report
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` — live bound proofs (`include!` from the lifecycle test crate)

Edit:

- `src/lib.rs` — private module
- `src/daemon_attach_stream.rs` — bound-handle enum; WebRTC bind helper beside `bind_unix_adapter_after_attaching`; shared capability intersection
- `src/daemon_transport.rs` — Attach bind when `grant_id` + WebRTC Hello feature; PeerClosed bound close-only; occupancy through live route set
- `src/local_webrtc.rs` — inbound Hello; register admission; sender-loop flush; encrypt + chunk as `DaemonTerminalFrame`; wrap `local_close` in `LOCAL_WEBRTC_PEER_CLOSE_BOUND`; hang inject; exhaustive delivery-kind matches; close adapters on `remove_peer`
- `src/local_webrtc_smoke.rs` — exhaustive delivery-kind match
- `crates/botster-hub-client/src/lib.rs` — `FEATURE_WEBRTC_TERMINAL_ADAPTER`, `for_webrtc_terminal_adapter()`, delivery kind, revision 40
- `crates/botster-hub-client/src/typescript.rs` + `crates/botster-hub-client/generated/daemon-protocol.ts`
- `packages/hub-test-support/*` — unpublished `0.1.35`; matrix lists the optional feature under supported, not required; sync generated protocol
- `tests/hub_daemon_lifecycle_test.rs` — `include!("hub_daemon_lifecycle/webrtc_terminal_adapter.rs")` next to the Unix include
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` — exhaustive `DaemonLocalWebrtcDeliveryKind` matches; unbound receive path must not require Hello
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` — keep unbound Drain proofs without Hello
- `docs/client-protocol.md` — DataChannel Hello, optional feature, delivery kind, bound close-only, DataChannel close bound
- `tests/hub_daemon_lifecycle/mod.rs` — already exports `webrtc_fixtures`; no new module unless a helper moves there

Do not edit `botster-web`. Do not vendor `daemon_terminal_frame` into that repo in this ticket.

Do not add Hub-owned adapter-law crates. Do not decode GHOSTSNP in tests.

## Risks

- Always-bind would break current Web Drain before the Web ticket.
- Encrypt/chunk/send inside `try_write` would block the Core host tick on DataChannel pressure. That is a Plan Review reject.
- A second send task would split peer ownership and leak on teardown (`[[webrtc peer cleanup removes every per peer owner together]]`).
- Mixing terminal frames into `DaemonResponse` deliveries would collapse the control plane.
- PeerClosed that still Hub-Detaches a bound route creates a second detach and can cancel a replacement owner.
- Occupancy `saturating_sub` without `live_attach_routes` leaves replacement Attach unable to become live.
- Terminal JSON or mux delivery alone can hide a live `PeerConnectionDriver` timeout storm.
- Adding the delivery-kind variant without a TypeScript and support-matrix update fails workspace fixtures.
- Putting the feature in `DaemonCompatibilityRequirement::current()` raises the client floor.
- `close()` that `block_on`s the DataChannel writer is a Plan Review reject.
- An unbounded `local_close().await` delays `cleanup_once`, so adapters and the peer runtime never reach the hard stop.
- Sending `daemon_terminal_frame` to current Web (`hub-test-support@0.1.32`) throws in `webrtcDaemonClient.ts`.

## Acceptance checks / tests

Repository gates, in this order (Plan Review reproduced worker-missing failures when the worker build was skipped):

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
BOTSTER_ENV=test cargo clippy --workspace --all-targets --locked -- -D warnings
./test.sh --locked
```

Do not run bare `cargo test`. Use `./test.sh` / `BOTSTER_ENV=test` wrappers (`[[test script required for rust tests not cargo test]]`).

After clippy is green, the worker binary must exist before `./test.sh --locked`. Record Hub SHA, locked Core SHA, and realpaths of both `botster-hub` and `botster-session-worker`.

Focused proofs Implement must add or update:

1. **Harness:** Hub `WebRtcTerminalAdapter` driver passes `assert_terminal_adapter_conformance` (bounds, order, typed rejection, no retry, content-blind `to_bytes`, local close and transport close during an active write). Same suite as Unix.
2. **Chunk order:** one opaque frame becomes one delivery; `chunk_index` is monotonic from 0; `message_id` is unique per frame; a second `try_write` is `Full` until the first delivery completes; completing twice does not duplicate chunks; frame N+1 cannot start until frame N completes.
3. **Production bind path:** live Hub + worker from locked Core `f4f6bf5`; real bootstrap, origin-mismatch reject, secret-mismatch reject, signal, DataChannel, encrypted Hello with the feature; Attach may return **only** `Attaching`; inventory `adapter_bound=true` with the live generation **and `capabilities ==` the set Hub passed**; later frames appear only as `DaemonTerminalFrame` chunks. Snapshot frames appear on the adapter only when the bound set contains `snapshot_delivery=ready_then_history`.
4. **One-frame exception + fail-closed:** happy Attach is Attaching-only; after bind, Drain has no terminal bodies. Unexpected pre-bind frame fail-closes. Document that `ticket_1786661010_198387` removes the exception.
5. **Negative content-blind:** Hub production sources and new tests do not branch on READY, PAGE, FINISH, or decode `GHOSTSNP`. Prefer compile/test source scans over comments.
6. **Peer loss (bound):** close the offer peer; Hub issues no Detach; Core inventory row gone via adapter `Closed`; session still listed; `has_live_peer` is false; dedicated runtime is parked when no peers remain; `cleanup_hub_detach` delta is 0; `bound_adapter_close` advances. Terminal JSON is collected but is not the oracle.
7. **Grant revocation (bound):** `remove_peer` / fail-closed sibling grant drop closes adapters owned by removed grants and does not Hub-Detach those bound routes. Prove one effective Core detach per grant.
8. **Explicit Detach (bound):** authorized Detach is forwarded; leftover adapter is closed; second Detach / close is idempotent; generation N cannot delete N+1.
9. **Replacement owner:** A binds, B replaces the same `(session_id, subscription_id)`, delayed A PeerClosed does not close B. Oracle is Hub ownership + `bound_adapter_close` delta 0 for B, not continued ciphertext.
10. **Unbound residual:** WebRTC Attach without the Hello feature still Drains Snapshot as today. `local_webrtc_peer_close_detaches_terminal_subscriptions` remains an unbound Hub-Detach proof.
11. **Late messages:** after PeerClosed, queued Attach / SubscribeEntities / Hello from that grant do not recreate adapters or steal B's route.
12. **Compatibility:** default requirement still accepts the previous descriptor; `for_webrtc_terminal_adapter()` rejects a daemon that lacks the feature; `PROTOCOL_VERSION` remains 7; feature is advertised, not required.
13. **Provenance:** record Hub SHA, locked Core SHA, and realpaths of the tested `botster-hub` / `botster-session-worker` binaries.
14. **Sibling fail-closed:** ultimate `PeerConnection::close` hang still bounds the control thread and drops the dedicated runtime; name the sibling-grant blast radius in the test.
15. **DataChannel close bound:** production-handler test injects a never-completing `local_close`. `cleanup_once` still runs within `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Bound adapter is Closed. Peer is gone. Runtime parks when no peers remain. Ablating the timeout must hang or miss those oracles.
16. **Test wiring:** `include!` the new file from `tests/hub_daemon_lifecycle_test.rs`. `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal` must run the new proofs. Unbound fixture receive matches stay explicit for `DaemonResponse` / `DaemonEntityFrame` and must not treat an unexpected `DaemonTerminalFrame` on an unbound attach as success.
17. **Downstream generated consumer (Hub-owned sites 1–2):** regenerate `crates/botster-hub-client/generated/daemon-protocol.ts`; `npm run sync` / `--check` in `packages/hub-test-support` at unpublished `0.1.35`; `node packages/hub-test-support/test.mjs` passes. Feature is supported, not required.
18. **Downstream current Web (read-only):** current packaged Web consumes `@trybotster/hub-test-support@0.1.32` and vendored `src/botster/generated/daemon-protocol.ts` (`daemon_response` \| `daemon_entity_frame` only). Run that consumer, or the existing live packaged-Web DataChannel attach Hub already launches, against this Hub **without** Hello. Prove Drain still delivers Snapshot / TerminalOutput and that the wire contains no `daemon_terminal_frame`. Do not implement the Web decoder. Do not bump the Web pin. A drift check of Web against the new 0.1.35 artifact is expected to fail and belongs to `ticket_1786661008_897067`.

Downstream proof required by charter: Hub imports Core's harness and binds through `CoreDaemon` on the production WebRTC signal/DataChannel/Attach path. Hub also proves the current Web consumer remains on unbound Drain. Authentic Ghostty decoder proof stays on the Web ticket and the integration ticket. Do not weaken those later proofs.

## Required docs

- `docs/client-protocol.md`: optional `webrtc_terminal_adapter`, DataChannel Hello as protocol admission, bind sequence including the one-frame Attaching exception, `DaemonTerminalFrame` chunking, bound peer-loss close-only, Drain residual until cold-cut.
- Implement report under `docs/reports/`.
- No plugin README. No crates.io / npm publish unless the in-tree support-matrix fixture must stay source-derived under a new unpublished version.

## Pipeline gates and artifacts

- Plan artifact: this file (revision 2).
- Vault checklist: reuse `checklist_1786704630_986228` from the first Plan visit; do not create a second ticket checklist.
- Gate evidence must include `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, `target_repository`.

## Vault gaps worth capturing

- After this ticket ships: capture that WebRTC protocol admission is DataChannel Hello, that WebRTC revocation is grant `remove_peer`, and that DataChannel `local_close` uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND` before `cleanup_once`.
- Whether Hello-on-DataChannel becomes the general WebRTC client-admission switch should be captured once Web consumes it.
- Do not capture the proposed north star as ratified from this ticket alone.
- Do not capture a new teardown lens. The existing class already covers this ticket.
