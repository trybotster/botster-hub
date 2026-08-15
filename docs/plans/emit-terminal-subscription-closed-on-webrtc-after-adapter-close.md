# Plan: Emit TerminalSubscriptionClosed on WebRTC after adapter close

Ticket: `ticket_1786724303_284888`
Run: `run_1786724337_992334`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Required by Web ticket `ticket_1786661008_897067` for authentic slow-client proof.
Unix already emits this event from closed ticket `ticket_1786705502_228757`.
Unix sibling `ticket_1786716545_417854` is mux-readable proof only. Do not treat it as the WebRTC oracle.
Plan **revision 2** after Plan Review `review_1786730592_600257`.

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786730592_294389` protocol-7 close-event delivery gate | product / high | Human answer `question_1786730510_751282`: keep protocol 7. Require `terminal_subscription_closed` on encrypted DataChannel Hello before Hub sends `daemon_event`. Unnegotiated protocol-7 clients never receive or decode the new kind. Load [[botster core public enums are breaking until non exhaustive is decided]] and [[public dto field additions are source breaking without non exhaustive]]. |
| `finding_1786730592_528268` downstream package handoff | product / high | Registered Hub publish ticket `ticket_1786730686_674642` on `tgt_7e208a0c76a44980a83b63af976b1f22`. That ticket depends on this ticket. Web ticket `ticket_1786661008_897067` depends on the publish ticket. Sibling `ticket_1786723348_522242` may still publish `0.1.35`. This run cuts source over to a new unpublished version so those publishes do not collide. |
| `finding_1786730592_791695` Plan completion evidence | process / low | Resubmit gate evidence with existing `artifact_1786729701_892215` and `checklist_1786724757_253940`. Do not create another plan artifact or vault checklist. |
| `finding_1786730592_687105` binary and strict gates | product / medium | Build locked `botster-session-worker` before live tests. Add `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`. Record Hub and locked-Core SHA plus binary realpaths. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` from `list_spawn_targets` |
| Plan worktree | this pipeline worktree |
| Worktree hygiene | tracked `.gitignore` has 53 bytes and is not empty; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `24517f4879a6effdd87eacddbb4b40aca13104c1` |
| Locked Core | `Cargo.lock` pins `botster-core` and `botster-terminal-protocol` at `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** |
| `teardown_class_applies` | **yes** — WebRTC peer lifecycle, adapter close, sibling-subscription isolation, and terminal-state versus live-runtime divergence |

Independent resolution: `project_pipelines_current_context` ticket and run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Routing did not use the process working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
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
- other repository charters — this run stays on `botster-hub`

Targeted notes:

- [[Unix mux host events are unsolicited control frames]]
- [[Unix mux host frames flush before new terminal slots]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[conformance fixture revisions must be unique per published content]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster hub is a first party host profile over core]]
- [[botster hub client crate is the external client boundary]]
- [[botster core public enums are breaking until non exhaustive is decided]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[rust repo strict lints must be verified before dismissing warnings]]

Human answer loaded:

- `question_1786730510_751282` — keep protocol 7; require explicit `terminal_subscription_closed` negotiation before `daemon_event`

## Context loaded

Ticket gap, confirmed in this worktree:

- Unix `UnixConnectionMux::queue_closed_subscription_events` emits `DaemonEvent::TerminalSubscriptionClosed` with `host_adapter_closed` or `core_adapter_closed`.
- Unix flush writes that event as a mux Event. The connection stays up.
- WebRTC `flush_webrtc_adapter_frames` skips closed handles and does not emit a host event.
- `WebRtcConnectionMux` stores `(session_id, subscription_id, handle)` only. It does not store generation, `host_closed`, pending events, suppress lists, or a dying flag.
- `BoundAdapterHandle::close_from_host` calls WebRTC `close()` and does not set a host-close flag.
- `DaemonLocalWebrtcDeliveryKind` is only `DaemonResponse`, `DaemonEntityFrame`, and `DaemonTerminalFrame`.
- IsolatedHub WebRTC helpers park entity and terminal deliveries. They do not consume `terminal_subscription_closed`.
- Adapter unit tests pass the Core conformance harness. They do not emit or consume the close event.

Web ticket `ticket_1786661008_897067` needs this Hub event for authentic slow-client proof. This run must not edit `botster-web`.

## Scope and non-scope

### Scope

1. When a bound WebRTC adapter closes from Core write-budget (`core_adapter_closed`) or host adapter close (`host_adapter_closed`), emit `DaemonEvent::TerminalSubscriptionClosed` with `session_id`, `subscription_id`, generation, and that reason.
2. Deliver the event on the WebRTC host plane of the same live peer.
3. Keep the DataChannel readable. A sibling subscription on the same peer must keep producing `daemon_terminal_frame`.
4. Do not require the client to stop reading the peer to create the Core close.
5. Do not emit on explicit Detach, peer death, `ProcessExited`, or session removal.
6. Add IsolatedHub proof that a hub-client consumer observes the typed event without stalling sibling frames.
7. Keep Hub content-blind. Pass the Core adapter conformance harness.
8. Merge directly into `main`. Do not create a PR.

### Default product decisions

| Topic | Decision |
| --- | --- |
| Host-plane delivery | Add `DaemonLocalWebrtcDeliveryKind::DaemonEvent`. Encrypt and chunk `DaemonEvent` the same way entity frames are sent. Do not reuse `daemon_response` (request-paired). Do not reuse `daemon_terminal_frame` (terminal plane). This is the WebRTC analog of `DaemonUnixMuxFrame::Event`. |
| Protocol-7 delivery gate | Human answer `question_1786730510_751282`. Keep protocol 7. Hub sends `daemon_event` only when the encrypted DataChannel Hello `required_features` includes `terminal_subscription_closed`. Store that bit on the admitted peer. Unnegotiated protocol-7 adapter clients stay on `daemon_response` / `daemon_entity_frame` / `daemon_terminal_frame` only. If Implement cannot enforce that gate, stop and bump protocol instead of sending an unknown enum variant. |
| Client helper | Add `DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed()` that requires both `webrtc_terminal_adapter` and `terminal_subscription_closed`. Do not add the close feature to `for_webrtc_terminal_adapter()` or to `DaemonCompatibilityRequirement::current()`. New Web and TUI clients must use the negotiated helper. This run does not edit those repos. |
| Protocol | Keep `PROTOCOL_VERSION` 7. Bump `CONFORMANCE_FIXTURE_REVISION` to 41. Do not add a new default-required feature. `terminal_subscription_closed` stays advertised and optional. |
| Reason set | Only `host_adapter_closed` and `core_adapter_closed`. No `process_exited` reason. |
| Host-close flag | Add `host_closed` on the WebRTC handle. `close_from_host` sets it only when the handle is not already closed. Core `close()` leaves it false. |
| Queue versus reconcile | Queue the close event from the live handle state before inventory sweep. Do not let `reconcile_inventory` rewrite a completed Core reason. |
| Flush order | Flush pending host events before a new terminal slot. Skip closed handles. Do not complete a closed slot as a terminal frame. |
| Peer death | `close_all` marks the mux dying and closes handles. Dying muxes queue zero close events. |
| Detach | Suppress that generation on the WebRTC mux. Do not emit. |
| Process exit / session removal | Suppress the session when Hub lifecycle already shows exit, failed, stopping, or missing. Do not emit. |
| npm publish | Cut `packages/hub-test-support` to a new unpublished version (0.1.36 or next unused). Sync generated assets. Do not publish in this ticket. Registered publish ticket `ticket_1786730686_674642` consumes this merge. Sibling `ticket_1786723348_522242` may publish 0.1.35 without `daemon_event`. |

### Non-scope

- Do not edit `botster-web`.
- Do not publish `@trybotster/hub-test-support` or `@trybotster/terminal-protocol`.
- Do not change Unix mux policy, Unix Event framing, or ticket `ticket_1786716545_417854`.
- Do not inspect READY, PAGE, FINISH, Snapshot, or GHOSTSNP bodies.
- Do not emit the close event as `daemon_terminal_frame`.
- Do not add Hub terminal-body translation or a second attach phase machine.
- Do not change Core write-budget (512 ticks). Core already owns that hard-stop.
- Do not cold-cut remaining Drain translation (`ticket_1786661010_198387`).
- Do not treat Unix mux-readable proof as the WebRTC oracle.
- Do not dual-pipeline this ticket.

## Repository ownership boundaries and cross-repo dependencies

Hub owns host events, WebRTC admission, adapter instances, framing, encryption, and transport writes.

Core owns terminal subscriptions, attach generations, write-budget hard-stop, and adapter `close()` on the host tick.

`botster-hub-client` in this repo owns the public host DTO, delivery-kind enum, generated TypeScript, and IsolatedHub consumer helpers.

Web consumes the event later. Do not edit Web here.

| Seam | Action |
| --- | --- |
| Closed Unix sibling `ticket_1786705502_228757` | Already on `main`. Copy the emit contract. Do not reopen Unix. |
| Open Unix sibling `ticket_1786716545_417854` | Mux-readable proof only. Not a dependency. Not the WebRTC oracle. |
| Open Hub publish `ticket_1786723348_522242` | Sibling on the same Hub target. It may publish `0.1.35` without `daemon_event`. Do not block it. Do not mutate `0.1.35` after it publishes. |
| New Hub publish `ticket_1786730686_674642` | Registered on `tgt_7e208a0c76a44980a83b63af976b1f22`. Depends on this ticket (`dependency_1786730696_284338`). Publishes the merged source that contains `daemon_event` and the negotiated-close contract. |
| Web consumer `ticket_1786661008_897067` | Downstream on `tgt_40abcf71ccf049f4ac0c99953a799869`. Depends on the new publish ticket (`dependency_1786730694_799927`). Do not edit Web here. Web must Hello-require `terminal_subscription_closed` before it decodes `daemon_event`. |
| Core | No Core ticket. Write-budget close already exists at locked rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`. |

This ticket has no blocking prerequisite. Downstream order is this merge → `ticket_1786730686_674642` → Web. Dependencies are registered against the Hub and Web targets, not against this ticket's target as a fallback.

## Assumptions and unknowns

1. Human answer `question_1786730510_751282` is the protocol decision. Protocol stays 7. The new enum variant is gated by Hello `required_features`.
2. IsolatedHub keep-reading proof is the required oracle. Status-on-timeout is not enough.
3. Fast local DataChannels may accept every terminal send. Implement must still create Core write-budget while the peer stays readable. Use per-handle Full retention and host-first flush. Do not use mux-wide `set_would_block`. Do not require the client to stop reading. If IsolatedHub loopback cannot Pending one handle without a new Hub terminal policy queue, stop and ask a human. Do not add a test-only skip of the production flush.
4. `CONFORMANCE_FIXTURE_REVISION` 41 is free. If publish history claims 41 before Implement lands, allocate the next unused revision.
5. This run always cuts hub-test-support source to a new unpublished version so sibling `0.1.35` publish cannot collide.
6. Session-type eligibility parent pins do not apply.

## Affected surfaces/files

| Path | Change |
| --- | --- |
| `src/webrtc_terminal_adapter.rs` | Add `host_closed`, generation-bearing routes, pending events, suppress lists, dying flag, `queue_closed_subscription_events`, `close_from_host` that does not rewrite an already-closed handle |
| `src/daemon_attach_stream.rs` | Pass generation into WebRTC `mux.register`. Route `close_from_host` to the WebRTC host-close path |
| `src/daemon_transport.rs` | Queue WebRTC close events on the control thread. Suppress Detach generations and shutdown/exit sessions on WebRTC muxes. Queue before reconcile |
| `src/local_webrtc.rs` | Flush host events before new terminal slots. Skip closed handles. Do not encode the event as `daemon_terminal_frame`. Keep sibling sends and inbound reads alive |
| `src/main.rs` | Existing operator log for the event stays shared |
| `crates/botster-hub-client/src/lib.rs` | Add `DaemonLocalWebrtcDeliveryKind::DaemonEvent` and `for_webrtc_terminal_subscription_closed()`. Keep protocol 7. Bump conformance to 41 |
| `crates/botster-hub-client/src/typescript.rs` | Add `daemon_event` to the generated union |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Park host-event deliveries. Add `next_host_event` |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | IsolatedHub emit, sibling, negative, and keep-reading write-budget proofs |
| `docs/client-protocol.md` | Document WebRTC host-event delivery and non-emit cases |
| `packages/hub-test-support/**` | Sync generated protocol and fixtures. Version cutover only if required |

Do not edit `botster-web` or Unix adapter policy files except shared helpers that already classify both transports.

## Implementation steps

1. Mirror the Unix mux record on `WebRtcConnectionMux`: generation, `reported`, pending events, suppress lists, dying.
2. Add `host_closed` and `close_from_host`. If the handle is already closed, leave the reason as Core.
3. Bind path: `mux.register(session_id, subscription_id, generation, handle)`.
4. On encrypted Hello, record `close_events_admitted` when `required_features` contains `terminal_subscription_closed`. Copy that bit onto the peer mux or admission record.
5. Control thread: `queue_webrtc_subscription_closed_events` beside the Unix queue. Call it after requests and on the bound-route pump. Queue before `reconcile_inventory`. Queue only for admitted peers.
6. Detach suppresses the live generation. Shutdown, process exit, and missing/non-running lifecycle suppress the session. Peer `close_all` sets dying.
7. Peer loop: if `close_events_admitted`, pop pending host events, frame them as `daemon_event`, send them first. If not admitted, drop queued close events and never write `daemon_event`. Then flush live sibling terminal slots. Skip closed handles.
8. Keep send-first arbitration and `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Do not close the peer because one adapter closed.
9. IsolatedHub consumer: classify `daemon_event` only in tests that negotiated the feature. Unnegotiated helpers must fail closed if they ever see the new kind.
10. Tests listed below. Ablate emit, sibling, and negotiation oracles so a revert goes red.
11. Cut hub-test-support to a new unpublished version. Sync generated TypeScript and assets. Do not publish.

## Runtime-teardown answers

| Field | Content |
| --- | --- |
| `teardown_class_applies` | yes. The ticket is WebRTC adapter close, peer-live host-event emit, sibling isolation, and write-budget hard-stop. |
| `teardown_isolation` | One closed generation dies. The peer, DataChannel, sibling subscriptions, host request path, and other peers stay up. Peer death is a separate path that marks the mux dying and emits nothing. |
| `teardown_bounds` | Adapter `close` stays non-blocking. DataChannel `local_close` stays inside `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. A Pending or abandoned terminal send must not `block_on` the control thread and must not call `close_all`. Only true peer/channel failure retires the peer. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | Live IsolatedHub peer: Core or host close → handle `is_closed` → control-thread queue → peer-loop host-event flush → `daemon_event` plaintext is `TerminalSubscriptionClosed` → sibling `daemon_terminal_frame` continues → DataChannel still accepts Status/ListSessions. A terminal JSON file or unit `close()` call is not enough. |
| `ownership_identity` | Owner key is session + subscription + generation + grant/peer. Stale generation N must not sweep live N+1. Delayed PeerClosed must not delete a replacement owner. |
| `sibling_fail_closed_policy` | On successful close of one generation: siblings keep working. On ultimate peer close failure: the whole peer retires once through existing cleanup. Other peers are untouched. Test both. |

### Late-message matrix

| Message | Tag | Reject after this generation is terminal | Residual sweep |
| --- | --- | --- | --- |
| Attach | client_id + grant_id + live generation | Fail-closed if admission/grant/peer is gone. Same-peer re-attach of one key may host-close generation N | Do not emit for Detach. Replacement generation N+1 stays bound |
| Drain | owner client/grant | No terminal bodies. Control-plane only | No close event from Drain |
| SendInput / Resize | host session | Stay available after one generation closes | No close event |
| Detach | session + subscription + live generation | `AlreadyGone` or generation mismatch | Suppress that generation. No emit |
| SubscribeEntities / UnsubscribeEntities | peer-owned subscription id | Drop frames the peer does not own. After peer death, no new durable owner | Existing peer cleanup. No `TerminalSubscriptionClosed` |
| Encrypted Hello | grant + peer + required_features | After `cleanup_sent` / dying, do not create a new live admission. `daemon_event` requires `terminal_subscription_closed` in Hello | No emit without that feature |
| DataChannel OnClose / peer death | peer id / grant | `close_all`, dying=true | No emit. One cleanup path |
| Grant `remove_peer` | grant id | Close bound adapters for that grant | Dying/peer-loss path. No emit |
| ShutdownSession / process exit / session removal | host session id | Lifecycle / `ProcessExit` only | `suppress_session`. No emit |
| Stale `TerminalSubscriptionClosed` for N after N+1 is live | generation | Ignore for N+1 | Must not close N+1 |
| Control request after peer death | peer gone | No durable ownership | Existing forget/remove path |

## Risks

1. Fast loopback DataChannels may complete every terminal send. Then IsolatedHub `yes` never reaches 512 Full ticks while the client keeps reading. Implement must prove keep-reading `core_adapter_closed` on the production flush path, or ask a human.
2. `reconcile_inventory` can rewrite Core close to host close if `close_from_host` sets the flag on an already-closed handle. Queue first. Do not set `host_closed` after Core close.
3. A new delivery kind is source-breaking for exhaustive Rust matches in this repo. Update IsolatedHub helpers in the same change.
4. Concurrent hub-test-support publish can freeze `0.1.35` or claim conformance 41. Follow unpublished-version and unique-revision rules.
5. Flushing host events after terminal slots can hide `core_adapter_closed` under flood. Host events go first.
6. Mux-wide `set_would_block` would stall siblings. Do not use it for this proof.
7. Unix Status-on-timeout helper can hide the queue/reconcile race. WebRTC proof must keep reading deliveries.

## Acceptance checks/tests

Production IsolatedHub WebRTC subscriber (hub-client peer, not Web):

1. Negotiated host close of one of two bound subscriptions emits exactly one `daemon_event` whose plaintext is `terminal_subscription_closed` with `host_adapter_closed`, identity, and generation. The peer stays up. Sibling still delivers `daemon_terminal_frame`. Status/ListSessions still work. Hello required `terminal_subscription_closed`.
2. Authentic Core write-budget hard-stop on a negotiated peer emits exactly one event with reason exactly `core_adapter_closed`. The observer keeps reading the peer. Sibling `daemon_terminal_frame` continues. Do not use Status-on-timeout as the only oracle.
3. Unnegotiated protocol-7 adapter Hello (`webrtc_terminal_adapter` only) never receives `DaemonLocalWebrtcDeliveryKind::DaemonEvent`. IsolatedHub receive path must not decode the new kind. Adapter bind and sibling terminal frames still work.
4. Explicit Detach does not emit.
5. Peer close / DataChannel death does not emit.
6. Process exit and `ShutdownSession` do not emit. Lifecycle / `ProcessExit` may still appear.
7. Failed `RemoveSession` does not suppress a later Core close.
8. Replacement owner: A receives close for generation N. B on the same session key stays bound and keeps terminal frames. Hub-visible generation/route occupancy is the ownership oracle. Mux delivery alone is not enough.
9. Hub remains content-blind. No READY/PAGE/FINISH/Snapshot decode on the emit path.
10. `assert_terminal_adapter_conformance` still passes.
11. Default `DaemonCompatibilityRequirement::current()` still accepts a daemon without a new required feature. `for_webrtc_terminal_adapter()` still omits the close feature. Protocol stays 7. Conformance becomes 41.
12. Ablation: remove emit, sibling flush, or the Hello gate and show the new proofs go red.

Repo gates:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./test.sh -p botster-hub-client
./test.sh --test hub_daemon_lifecycle_test webrtc_terminal_adapter
./test.sh -p botster-hub webrtc_terminal_adapter
```

Record `CARGO_BIN_EXE_botster-hub` realpath, `botster-session-worker` realpath, Hub SHA, and locked Core SHA `f4f6bf5babe92dfb9241a760c414187f711c2c42`. Also run the Unix close tests only if a shared helper changes.

## Vault gaps worth capturing

Capture after Implement if Review agrees:

1. WebRTC host events use a `daemon_event` delivery kind and are unsolicited. They are not request replies and not terminal frames.
2. Protocol 7 may add that kind only after Hello negotiates `terminal_subscription_closed`.
3. WebRTC `close_from_host` must not set `host_closed` on an already-closed handle. Queue before reconcile.

Do not capture during Plan. The Unix Event convention already exists. This ticket extends it to WebRTC.

## Review / Verify overlays

Review loads [[botster-runtime-reviewer-playbook]] and the teardown lenses.

Verify loads [[botster-runtime-verifier-playbook]]. Verify must keep a keep-reading IsolatedHub observer, sibling `daemon_terminal_frame`, exact `core_adapter_closed`, and live peer readability.

## Product decision ledger

| Item | Status |
| --- | --- |
| `daemon_event` delivery kind | Default. Analog of Unix Event. |
| Protocol 7 + Hello gate | Human answer. Do not send the new kind without `terminal_subscription_closed` in Hello. |
| Protocol 7 / conformance 41 | Additive host-plane shape. |
| No Web edit | Required by ticket. Web consumes published package via `ticket_1786730686_674642`. |
| No npm publish in this ticket | Registered follow-on `ticket_1786730686_674642`. |
| Ask human | Only if IsolatedHub cannot prove keep-reading write-budget without a new Hub terminal policy queue, or if the Hello gate cannot be enforced. |
