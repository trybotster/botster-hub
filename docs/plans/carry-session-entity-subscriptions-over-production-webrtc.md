# Carry session entity subscriptions over production WebRTC

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Target resolution: Project Pipelines run `run_1784768142_218672` names that target id, and the admitted spawn-target registry maps it to `trybotster/botster-hub`. The assigned worktree is on `project-pipelines/ticket_1784768098_321065` at main commit `02bffeb`; the ambient directory was not used to infer ownership.
- Repository charter: [[botster-hub-playbook]]. The affected repository-owned layers are the Rust daemon transport, local WebRTC peer adapter, the externally consumed `botster-hub-client` crate, generated TypeScript, hub test support, and the npm release artifact.
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[botster-hub-client-playbook]].
- Architecture maps and planner context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[botster pipeline needs continuous product owner between agent steps]].
- Repository-boundary notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster hub events use bounded priority lanes instead of unbounded queue fuses]].
- Ticket-specific notes loaded: [[botster hub client state sync is entity frame only]], [[botster client subscriptions should not hydrate global state]], [[botster entity snapshots are authoritative reconnect baselines]], [[scoped entity snapshots preserve whole-family sequence gates]], [[webrtc peer registry owns production data plane receivers]], [[webrtc peer cleanup removes every per peer owner together]], [[late webrtc messages after disconnect must not recreate clients]], [[webrtc e2e encryption now mandatory no plaintext paths]], [[snapshots-delivered-as-atomic-webrtc-messages]], [[packaged botster web reloads need fresh webrtc grants]], [[adding a hub client feature constant is a three site change]], [[generated typescript dtos must encode serde field optionality]], [[external client hub tests use subprocess spawned hub test support]], [[hub test support npm releases need external consumer smoke]], and [[backpressure recovery tests must cover empty and failed snapshot branches]].
- Workflow and verification notes loaded: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- [[project-pipelines-playbook]] was intentionally not loaded: Project Pipelines is orchestrating this run, but the ticket does not change its package/plugin paths or workflow policy.

## Current repository evidence

- `src/daemon_transport.rs` already owns the authoritative, connection-scoped `session` subscription over the daemon socket. It registers through `ControlMessage::SubscribeEntities`, emits an initial snapshot from `CoreDaemon` lifecycle truth, drives ordered upsert/patch/remove frames, uses a bounded 64-frame subscriber queue, resnapshots after overflow, and removes subscriptions on explicit unsubscribe or socket loss.
- `src/local_webrtc.rs` is the production local browser adapter. It admits one ordered/reliable DataChannel per fresh bootstrap grant, decrypts the existing `DaemonRequest`, submits ordinary requests to the daemon owner, and serializes one encrypted `DaemonResponse` at a time into bounded `DaemonLocalWebrtcResponseChunk` frames.
- The WebRTC path currently routes every request through `ControlMessage::Request`; `handle_control_request` intentionally rejects `SubscribeEntities` and `UnsubscribeEntities` because the socket path handles streaming outside ordinary request/response dispatch. The peer therefore has no entity receiver and no way to emit an unsolicited `DaemonEntityFrame`.
- The current DataChannel sender has one bounded inbound request FIFO and explicit high/low watermarks, but it only wakes for channel or peer-terminal events. Entity delivery needs to join that same per-peer sender loop so chunks from two logical deliveries never interleave and no second unbounded egress path bypasses its pressure policy.
- Peer cleanup already records and detaches terminal attach subscriptions, but it does not track entity subscription ids. The cleanup message and idempotent peer teardown are the correct place to remove every peer-owned entity subscription as well.
- `crates/botster-hub-client` already defines `SubscribeEntities`, `UnsubscribeEntities`, `DaemonEntityFrame`, feature metadata, and generated TypeScript. The missing browser contract is a typed local-WebRTC delivery discriminator that lets one DataChannel distinguish a correlated `DaemonResponse` message from an unsolicited entity message after authenticated decryption.
- `crates/botster-hub-test-support` already proves the socket entity lifecycle contract in the real isolated Hub/Core/session-worker topology and publishes the current response-chunk and session-lifecycle fixtures. The real WebRTC harness in `tests/hub_daemon_lifecycle_test.rs` already proves encrypted requests, chunk reassembly, fresh grants, reconnect, request traffic, and terminal cleanup, but not pushed entity frames.
- The public npm registry was checked during planning: `@trybotster/hub-test-support@0.1.9` is current, with integrity `sha512-l8521hb0K2KszUM9io3T6U+K1EiLiTSpV2Fq+wS3CSJ/Bh4tF1HH/8YESlEyIsx/sdwG7dLpmHJpwSJzIU+VRw==`. Implementation must recheck immediately before allocating the next version; `0.1.10` is the expected next coordinate if the registry is unchanged.
- This repository's active durable plan convention is `docs/plans/`, confirmed from current mainline prior art.

## Scope

1. Route `SubscribeEntities { entity_type: "session", subscription_id }` and matching unsubscribe through the existing daemon-owner subscription registry when they arrive on an admitted local WebRTC peer.
2. Multiplex the resulting authoritative `DaemonEntityFrame` stream with ordinary encrypted `DaemonRequest`/`DaemonResponse` traffic on that same ordered DataChannel.
3. Give every outbound logical message a typed, authenticated delivery kind (`daemon_response` or `daemon_entity_frame`) and preserve current chunk bounds, AES-GCM protection, FIFO response correlation, and non-interleaving message assembly.
4. Reuse one bounded per-subscription entity queue and the existing per-peer DataChannel watermark policy. A slow browser must trigger the existing snapshot resync/close semantics without blocking the daemon owner or corrupting ordinary responses.
5. Track entity subscription ownership on the WebRTC peer and remove it on explicit unsubscribe, DataChannel close/error, peer failure, send failure, pressure timeout, daemon shutdown, and every partial-registration error.
6. Update the Rust-owned public contract, generated TypeScript, source-derived fixtures/support matrix, docs, npm package assets/version, and publication evidence.
7. Add real packaged-browser-shaped proof over `HubDaemon -> HubRuntime -> CoreDaemon -> botster-session-worker -> local WebRTC DataChannel`, including two reconnect cycles.

## Non-scope

- No changes to `botster-core`: it already owns lifecycle truth, generation-aware cursors, and the bounded lifecycle journal used by the hub projection.
- No new lifecycle watcher, duplicated entity projection, list/poll refresh loop, SSE transport, WebSocket fallback, or compatibility branch for the old direct-response-only DataChannel framing.
- No changes to terminal byte, scrollback, snapshot, file-transfer, or ClientWorker/SessionIo ownership. This ticket carries control-plane session entities, not terminal data-plane payloads.
- No new entity families, plugin entity generalization, UI state model, route hydration, or subscribe-time global state. Only the explicitly requested built-in `session` family is carried.
- No `botster-web` application implementation in this repository run. The hub must publish a consumable contract and prove it with a clean downstream-shaped consumer; wiring the browser store belongs to the `botster-web` target after this producer artifact exists.
- No broad `daemon_transport.rs` or `local_webrtc.rs` refactor, new crate, new queue dependency, optional tunables, or adjacent cleanup.

## Ownership boundaries and cross-repository dependencies

- `botster-core` remains the authority for session/process lifecycle, source generations, cursors, and resync reasons. This run consumes existing APIs and must not copy or reinterpret that truth.
- `botster-hub` owns WebRTC admission, subscription authorization, mapping one peer to its owned subscription ids, delivery ordering, bounded egress/backpressure, diagnostics, resync policy, disconnect cleanup, and production wiring.
- The in-repository `botster-hub-client` crate owns the external serde DTO, WebRTC delivery/chunk discriminator, constants, compatibility metadata, and generated TypeScript. Browser-specific stores and presentation remain outside it.
- `botster-hub-test-support` owns reusable live-hub conformance and the npm artifact. Its runtime proof must use real hub and session-worker subprocesses rather than a duplicate fake transport.
- `botster-web` target `tgt_40abcf71ccf049f4ac0c99953a799869` is the downstream consumer. It is not a blocking prerequisite for this producer run, so no dependency is registered against it. A separate consumer ticket should depend on the published hub artifact/version produced here; this run must leave durable registry coordinates, tarball integrity, and an external-import smoke for that dependency.
- There is no open cross-repository prerequisite. If implementation discovers that Core cannot preserve ordering/resync semantics needed by the existing daemon registry, stop and register a dependency ticket against the `botster-core` target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` rather than adding hub-local lifecycle truth.

## Contract decisions

- Replace the response-only outer frame with one generalized local-WebRTC delivery chunk contract. Each chunk carries the existing version/id/index/count/size/payload fields plus an explicit delivery kind. Both `DaemonResponse` and `DaemonEntityFrame` plaintexts are serialized and AES-GCM encrypted before chunking; subscription ids and entity contents never appear in a plaintext fallback.
- Treat this as a cold-turkey local WebRTC framing revision. Increment the dedicated local WebRTC chunk/delivery version and update all producer, harness, generated TypeScript, fixture, and package consumers together; do not support both response-only and multiplexed decoders. The global daemon request/frame vocabulary already contains subscriptions and entity frames, so only bump `PROTOCOL_VERSION` if implementation changes that global handshake contract. Allocate a new globally unique conformance fixture revision for changed published bytes.
- Keep response correlation exactly one request FIFO to one complete `daemon_response` message. A hub-minted `message_id` identifies all chunks of that response. Entity messages use their own message ids and may appear between complete responses, never between chunks of one response.
- Route subscribe outside `handle_control_request`, as the socket transport already does. Register the bounded frame sender with the daemon owner, emit the `EntitySubscribed` response completely, then emit the queued authoritative snapshot before later deltas. Reject unsupported families and duplicate ids through the existing typed operator-error path.
- Give the async WebRTC peer an awaitable bounded entity receiver while preserving the socket consumer. Prefer the already-depended-on Tokio bounded channel (async `recv` for WebRTC and blocking receive on the socket thread) or an equally small standard-library bridge; do not add a new queue abstraction or polling loop. The daemon owner must retain `try_send` semantics.
- Use one sender arbitration loop per peer. It observes inbound requests, peer terminal state, DataChannel high/low-water events, and entity deliveries; it completes one encrypted logical message before choosing the next. Ordinary responses should remain usable under an active subscription, while entity overflow remains visible through an authoritative `subscriber_overflow` snapshot rather than silently dropping a delta.
- Accept entity frames only for the currently active subscription id/generation on the peer. A reconnect starts with a fresh bootstrap grant, peer, subscription id, and snapshot. No frame buffered by a prior peer may be delivered or accepted on the next generation.
- Extend peer cleanup with entity subscription ids and invoke the existing `UnsubscribeEntities` daemon-owner path exactly once per id. Preserve idempotence when DataChannel and peer callbacks race, and keep late messages from recreating a removed peer or subscription.
- Keep diagnostics bounded and sanitized. Record delivery kind and terminal cause/counters without persisting grant secrets, encrypted payloads, entity bodies, absolute paths, or raw subscription contents. Existing `resync_reason: "subscriber_overflow"` remains the client-visible loss recovery signal.

## Implementation sequence

1. **Publish the generalized delivery DTO.** Add the delivery-kind enum/generalized chunk DTO and encrypt/chunk helpers in `botster-hub-client`/`src/local_webrtc.rs`; update exhaustive serde and TypeScript generation tests. Keep frame and assembled-message limits unchanged unless runtime evidence requires a separately reviewed change.
2. **Share the bounded entity sender seam.** Adapt `ControlMessage::SubscribeEntities` only as needed so the daemon owner can `try_send` into both the existing socket stream and an awaitable WebRTC receiver without changing lifecycle projection semantics.
3. **Register WebRTC entity subscriptions.** Special-case subscribe/unsubscribe in the peer loop, wait for the owner acknowledgement, record ownership only after successful registration, and order the response before the initial snapshot. Do not route the streaming request through generic `handle_control_request`.
4. **Multiplex outbound logical messages.** Extend the single peer sender arbitration to drain complete response or entity messages through the same encryption, chunk framing, high/low watermarks, bounded inbound request FIFO, and terminal cleanup. Prove chunks from different message ids/kinds cannot interleave.
5. **Complete cleanup and diagnostics.** Include peer-owned entity ids in idempotent close cleanup, remove them from the daemon registry, count/record entity delivery failures, and cover close/error/pressure/partial-registration races.
6. **Prove the production path.** Extend the existing Rust WebRTC offerer harness to demultiplex/decrypt both delivery kinds and run the existing session-lifecycle conformance scenario through the real daemon/Core/session-worker topology while issuing ordinary status/list or other correlated requests on the same peer.
7. **Regenerate and release test support.** Generate the TypeScript and npm assets from Rust source, add a WebRTC multiplex conformance fixture/metadata entry, bump the unique conformance revision and npm package version, run pack/external-install proof, publish the public package, then record the final registry coordinate, tarball URL, and `dist.integrity` for `botster-web`.
8. **Update protocol docs.** Document authenticated delivery kinds, correlation, ordering, bounds, subscribe/ack/snapshot ordering, overflow resync, cleanup, reconnect generation rules, and the absence of SSE/polling fallback.

## Affected surfaces and likely files

- `src/local_webrtc.rs` — per-peer subscription ownership, sender arbitration, delivery encryption/chunking, flow control, cleanup, and focused unit tests.
- `src/daemon_transport.rs` — shared bounded subscription sender/receiver seam, WebRTC subscribe/unsubscribe owner messages, cleanup, and diagnostics; the existing lifecycle projection should otherwise remain unchanged.
- `crates/botster-hub-client/src/lib.rs` — delivery kind/chunk serde DTOs, constants/revisions, examples, and symmetric contract tests.
- `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts` — generated browser-consumable transport contract and drift guard.
- `tests/hub_daemon_lifecycle_test.rs` — generalized WebRTC demultiplexer and real topology acceptance covering lifecycle, coexistence, pressure, cleanup, and reconnect.
- `crates/botster-hub-test-support/src/lib.rs` and `crates/botster-hub-test-support/examples/node_package_assets.rs` — source-derived WebRTC multiplex fixture/runner metadata and package emitter.
- `tests/hub_test_support_conformance_test.rs` — Rust-side package/conformance drift proof if the support surface changes.
- `packages/hub-test-support/{package.json,README.md,index.js,index.d.ts,test.mjs,metadata.json,daemon-protocol.ts,first-party-client-support-matrix.json}` plus a generated WebRTC multiplex fixture — regenerated npm package surface, tests, version, and docs.
- `packages/hub-test-support/scripts/sync-assets.mjs` — generator/check wiring for the new source-derived asset; generated files must not be edited manually.
- `README.md` and `docs/client-protocol.md` — production behavior and browser transport contract.
- `Cargo.lock` only if normal dependency resolution changes it; no dependency addition is planned.

## Assumptions and unknowns

- Assumption: “production WebRTC” means the admitted installed-`botster-web` local DataChannel implemented by `src/local_webrtc.rs`, not cloud/public signaling. This follows the ticket's packaged-browser topology and the repository's one-product-path documentation.
- Assumption: “two WebRTC reconnect cycles” means initial peer plus two fresh replacement peers. Each replacement obtains a new one-shot bootstrap grant and a new caller-supplied entity subscription id, receives a fresh authoritative snapshot, and rejects/ignores prior-generation frames.
- Assumption: one active built-in session entity subscription per peer is sufficient for the first-party browser contract. The daemon registry can still reject globally duplicate ids; the plan does not generalize multiple families.
- Assumption: the outer delivery kind may be visible because it contains routing metadata only; the response/entity body remains authenticated and encrypted. If review requires even the kind to be authenticated-only, place it inside the encrypted plaintext envelope while retaining an unambiguous post-decrypt union. Do not expose subscription ids outside encryption.
- Assumption: the next package release is `0.1.10` and the next conformance revision is `17` based on verified main/registry state. Implementation must recheck both immediately before generation/publication and use the next unused values if another release lands.
- Unknown: whether a generalized DTO should retain the historical `DaemonLocalWebrtcResponseChunk` name or be cold-turkey renamed to `DaemonLocalWebrtcDeliveryChunk`. Choose the name that truthfully models both kinds and update all in-repo consumers together; do not carry aliases or version-suffixed parallel types.
- Unknown: the smallest clean wakeup primitive for the current WebRTC crate's poll loop. Prefer existing Tokio/std primitives and preserve a single peer sender; if the crate cannot safely select an entity receiver with channel lifecycle events, document the exact limitation before choosing a bounded bridge thread.
- No ticket ambiguity requires a human decision before implementation. The accepted semantics fix authority, encryption, ordering, bounds, fresh generations, no fallback, and downstream artifact proof.

## Risks and mitigations

- **Response/entity chunk interleaving:** two senders could corrupt reassembly. Keep exactly one per-peer sender and finish every logical message before starting another.
- **Subscribe acknowledgement race:** an initial snapshot can become observable before the browser knows registration succeeded. Queue it in the bounded subscription receiver, send the correlated `EntitySubscribed` response first, then begin entity delivery.
- **Ordinary requests starve behind entity churn:** define bounded arbitration that continues accepting the existing request FIFO and prioritizes completing correlated responses without dropping entity state; overflow converges through a snapshot.
- **Slow browser blocks the daemon owner:** retain `try_send`, bounded per-subscription queues, DataChannel watermarks, and fail-closed peer cleanup. Never await browser I/O on the daemon owner thread.
- **Silent delta loss:** any full subscription queue must move that subscriber to resync-required state; the next authoritative recovery is a snapshot with `subscriber_overflow`, and failed delivery closes/removes the subscription.
- **Mixed reconnect generations:** bind subscriptions to the peer object, remove them on every terminal path, use a fresh id per replacement peer, and make the harness inject/replay an old-generation frame to prove rejection.
- **Cleanup races:** peer callbacks and DataChannel close can fire concurrently. Preserve the existing atomic cleanup-once gate and extend the one owner message to terminal plus entity subscription cleanup.
- **Security regression:** a plaintext entity frame or error fallback would bypass the established WebRTC boundary. Assert that every response/entity payload decrypts from AES-GCM and that malformed/unauthenticated frames fail closed.
- **Fixture/package drift:** Rust DTOs, generated TypeScript, Rust fixtures, npm copies, README version, package metadata, tarball, and public registry can diverge. Generate from Rust, run check mode, pack and install outside the source tree, then compare registry integrity.
- **Hub gravity:** generic transport orchestration could expand `local_webrtc.rs` further. Keep the change limited to one delivery union and existing daemon subscription owner; split modules only if required to keep the sender state machine testable, not as an adjacent refactor.
- **Real-topology flakiness:** subprocess/WebRTC tests can race startup and lifecycle. Reuse the serialized daemon test guard, explicit readiness, bounded timeouts, deterministic commands, and cleanup guards; do not use sleeps as the success oracle.

## Acceptance checks and downstream proof

1. `./test.sh -p botster-hub-client` proves serde stability for both delivery kinds, identical single/multi-chunk validation, generated TypeScript exactness, symmetric field/type/optionality checks, and the intended compatibility/conformance revision.
2. Focused `src/local_webrtc.rs` unit tests through `./test.sh -p botster-hub <filter>` prove one sender never interleaves message ids/kinds, request FIFO correlation survives entity traffic, bounded request/entity pressure is handled, invalid encrypted input fails closed, and cleanup is idempotent.
3. Focused daemon tests prove WebRTC subscribe authorization uses the same `session` registry as the socket path, unsupported/duplicate subscriptions return typed errors, ack precedes snapshot, unsubscribe removes the registry entry, and no status/package/worktree/plugin state is implicitly hydrated.
4. One real `hub_daemon_lifecycle_test` starts the actual `HubDaemon/CoreDaemon/session-worker` and packaged `botster-web` local bridge, opens the production DataChannel, and proves in order: empty snapshot, spawn upsert, starting/running/exited lifecycle patches, explicit remove, and visibility of a session spawned through an external daemon-socket client.
5. On the same active peer, interleave ordinary `Status`, `ListSessions`, and at least one mutating request with unsolicited entity delivery. Each request receives exactly one correlated complete response while entity sequence/order remains valid.
6. Force the bounded entity receiver into overflow and prove an authenticated authoritative snapshot with `resync_reason: "subscriber_overflow"` precedes later deltas; cover an empty recovery snapshot and failed/closed recovery delivery. A healthy peer/request path must remain usable.
7. Close the DataChannel without explicit unsubscribe and prove peer count, entity subscription count, terminal subscriptions, queued requests/deliveries, and sender tasks return to baseline. Repeat for send failure/peer failure so every terminal path is covered.
8. Perform initial connection plus two fresh reconnect cycles. Each replacement uses a new bootstrap grant and entity subscription id, begins with a fresh snapshot of current sessions, accepts later current-generation deltas, and rejects a prior-generation frame. Ordinary request/response remains usable on each cycle.
9. `./test.sh -p botster-hub-test-support`, `node packages/hub-test-support/scripts/sync-assets.mjs --check`, and `node packages/hub-test-support/test.mjs` prove source-derived Rust/npm asset parity, delivery fixture semantics, and support-matrix metadata.
10. Pack with `npm pack --dry-run`/`npm pack`, install the tarball in a clean temporary consumer outside the source tree, import the package, read the generated daemon protocol and new WebRTC fixture, run `verifyPackageAssets()`, and record the tarball SHA-512 integrity. This is the downstream-shaped `botster-web` proof required by the repository charter.
11. Recheck the public registry, publish the next unused `@trybotster/hub-test-support` version with public access, then capture `npm view` evidence for exact version, tarball URL, and `dist.integrity`. Do not claim publication from a local pack alone.
12. Run full repository gates: `cargo fmt --all -- --check`, `./test.sh`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `git diff --check`. Attribute any failure exactly; pre-existing failures are not blanket waivers.
13. The implementation report must trace the live path from encrypted `SubscribeEntities` on the DataChannel through WebRTC admission, daemon-owner registration, `HubRuntime/CoreDaemon` lifecycle baseline and changes, bounded entity queue, generalized encrypted delivery chunks, browser-shaped demultiplex/decrypt, and disconnect cleanup. Type existence or generated artifacts alone are insufficient.

## Required pipeline artifacts and gates

- Keep this plan committed and update it if implementation proves a materially different contract, file boundary, package coordinate, or executable check.
- Implementation evidence must include focused/full command outputs, the framing and conformance revision decision, runtime-path trace, queue/cleanup diagnostics, generated diff, tarball contents/integrity, clean-consumer smoke, registry publication coordinates, and any cross-repo follow-up ticket.
- Review and Verify must load the hub, runtime, and hub-client overlays; inspect every error/overflow/reconnect/cleanup branch; scan committed artifacts for secrets, local paths, or PII; and reject plaintext fallback, SSE/polling fallback, dead DTOs, or a subscription path not wired into the real DataChannel.

## Vault gaps worth capturing

- Capture after implementation if generalized encrypted WebRTC delivery chunks become the durable rule for multiplexing correlated responses and unsolicited entity frames without chunk interleaving.
- Capture after implementation if the settled sender arbitration rule (response completion, entity fairness, overflow-to-snapshot) is reusable beyond session entities.
- Capture the exact published package coordinate/integrity only if release evidence reveals a reusable project gotcha; transient release coordinates belong in the implementation artifact and repository README, not as a standalone architecture note.
- No Plan-time vault write is needed. Existing notes already constrain lifecycle authority, entity reconnect baselines, bounded recovery, peer cleanup, encryption, generated DTOs, release smoke, and runtime verification.
