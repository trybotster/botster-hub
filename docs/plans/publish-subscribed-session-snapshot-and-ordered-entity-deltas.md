# Publish subscribed session snapshot and ordered entity deltas

## Target and context

- Target repository: `botster-hub`.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Target worktree: the Project Pipelines worktree for this ticket, based on clean `botster-hub` main commit `4498fbb`.
- Repository charter: [[botster-hub-playbook]]. The affected Botster layers are the Rust HubRuntime facade, HubClientApi, daemon Unix-socket transport, the serde-only `botster-hub-client` boundary, generated TypeScript, and subprocess-backed hub test support.
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[botster-hub-client-playbook]].
- Architecture maps and required planner context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Repository-boundary notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster hub events use bounded priority lanes instead of unbounded queue fuses]].
- Ticket-specific notes loaded: [[botster hub client state sync is entity frame only]], [[botster client subscriptions should not hydrate global state]], [[botster entity snapshots are authoritative reconnect baselines]], [[scoped entity snapshots preserve whole-family sequence gates]], [[daemon socket attach must detach subscriptions on disconnect and exit]], [[botster hub client crate is the external client boundary]], [[botster hub client compatibility descriptors belong in client crate]], [[adding a hub client feature constant is a three site change]], [[daemon event shape changes bump conformance fixture revision not protocol version]], [[conformance fixture revisions must be unique per published content]], [[generated typescript dtos must encode serde field optionality]], [[generated dto drift tests need symmetric field and type checks]], [[external client hub tests use subprocess spawned hub test support]], [[published capability matrices must derive enumerations from source]], and [[backpressure recovery tests must cover empty and failed snapshot branches]].
- Workflow and verification notes loaded: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[rust repo strict lints must be verified before dismissing warnings]].
- [[project-pipelines-playbook]] was intentionally not loaded: this ticket is executed by Project Pipelines, but it does not change Project Pipelines package/plugin paths or workflow policy.

## Current repository evidence

- The production path is `HubDaemon` / `HubRuntime` -> `CoreDaemon` -> `botster-session-worker`; `README.md` explicitly rejects a parallel hub lifecycle authority.
- `Cargo.lock` currently pins botster-core at `8abfb1f`, before the closed prerequisite. Core main now contains merge `879f55e` for dependency ticket `ticket_1784752211_142730` and exports `SessionLifecycleBaseline`, `SessionLifecycleCursor`, ordered `SessionLifecycleChange`, and explicit `SessionLifecycleResyncReason` from `CoreDaemon`.
- Core's lifecycle source is bounded and generation-aware. It emits full-record upserts and removals; the hub must project those into the existing `session` entity family without recreating lifecycle truth.
- `HubRuntime` currently exposes `list_sessions`, spawn, attach/detach, drain, shutdown, and readback, but no lifecycle-baseline/change facade or explicit remove/forget facade.
- `HubClientApi` and `botster-hub-client` currently expose request/response session lists and terminal attach events. `DaemonConnection` is one-request/one-response; there is no held-open entity subscription helper or pushed entity frame contract.
- `src/daemon_transport.rs` owns each socket connection and already tracks terminal subscriptions for disconnect cleanup, but its owner loop has no session projection subscription registry or bounded per-subscriber delivery queue.
- Core records a natural process exit while the host drives `CoreDaemon::drain`. A hub projection pump must therefore preserve any terminal egress returned by that drain; polling only `lifecycle_changes` would not discover natural exits, and discarding drained egress would regress terminal subscribers.
- Core already owns the canonical `EntityFrame` vocabulary: `entity_snapshot`, `entity_upsert`, `entity_patch`, and `entity_remove`, with `entity_type`, `snapshot_seq`, and family-specific payload fields. The hub-client crate intentionally has no dependency on full hub/core internals, so its serde DTOs should mirror this wire vocabulary narrowly rather than import runtime types.
- The active repo plan convention is `docs/plans/`; this file follows current mainline prior art.

## Scope

1. Consume the merged CoreDaemon lifecycle source through a narrow HubRuntime facade and update `Cargo.lock` to the tested Core prerequisite revision.
2. Add an explicitly requested, connection-scoped subscription for the built-in `session` entity family. Subscription establishes only this requested family; it must not hydrate status, packages, worktrees, spawn targets, plugin entities, UI trees, or other global state.
3. Return a deterministic authoritative `entity_snapshot` first, then publish strictly ordered `entity_upsert`, `entity_patch`, and `entity_remove` frames derived from later Core lifecycle cursors.
4. Keep the hub's projection sanitized and policy-bearing while treating CoreDaemon as the only lifecycle authority.
5. Make delivery bounded per subscriber, isolate concurrent subscribers, resnapshot explicitly after overflow/loss, clean resources on disconnect, and require a new subscription id and baseline after reconnect.
6. Extend the public `botster-hub-client` serde contract, checked generated TypeScript, compatibility descriptor/support matrix, docs, and downstream-shaped subprocess conformance proof.
7. Retain `list_sessions` as an operator/query request while proving the subscription path does not use it as its synchronization loop.

## Non-scope

- No changes to `botster-core`; its closed dependency supplies the reusable lifecycle mechanism.
- No second lifecycle registry, process watcher, polling of `list_sessions`, bespoke `session_changed` event family, legacy list-refresh fallback, or dual old/new synchronization mode.
- No terminal byte, scrollback, snapshot, or file-payload ownership move into the hub projection. Existing SessionIo/ClientWorker delivery remains authoritative.
- No subscription to unrelated built-in or plugin entity families, no UI-tree/surface redesign, and no Project Pipelines plugin work.
- No botster-web or botster-tui implementation changes in this repository run. Their adoption of the generated contract is downstream work; this run provides the authoritative artifact and hub-owned conformance proof.
- No broad daemon transport rewrite, new async runtime, or new dependency solely for queues/ids. Prefer existing standard-library channels, mutexes, and the current owner-thread topology.

## Ownership boundaries and dependencies

- `botster-core` owns worker/process lifecycle truth, source generation identity, monotonic cursors, bounded lifecycle journal, and explicit source-change/cursor-expiry signals. Closed prerequisite: `ticket_1784752211_142730`, target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` (`botster-core`). The implementation must update the lockfile and compile against the merged API rather than copying it.
- `botster-hub` owns authorization, the sanitized session projection, subscription registration/lifetime, connection cleanup, bounded per-subscriber delivery, overflow diagnostics/resnapshot policy, and production daemon wiring.
- `botster-hub-client` (the crate inside this repository) owns public request/frame DTOs, the held-open connection helper, feature/compatibility metadata, conformance revision, and generated TypeScript.
- SessionIo/ClientWorker continue to own terminal egress. If the projection pump calls CoreDaemon drain to observe exit, it must route or retain returned terminal egress for its intended terminal subscription instead of consuming it as projection-only work.
- `botster-hub-test-support` owns the external-client subprocess proof and first-party support matrix. It may expose a narrow session-entity conformance result/fixture, but no runtime policy.
- `botster-web` and `botster-tui` are downstream consumers, not blocking prerequisites for this Hub producer ticket. A follow-up ticket should refresh their generated artifact and replace list-refresh synchronization with the new subscription; this run must not silently edit those repositories.

## Contract decisions

- Add a dedicated entity-subscription request/helper rather than interleaving unsolicited frames into ordinary `DaemonConnection::request` responses. The held-open stream completes the normal hello/compatibility handshake, registers one caller-supplied fresh `subscription_id` for `entity_type = "session"`, receives the initial frame, and then reads server-published entity frames until unsubscribe, EOF, resync failure, or daemon shutdown.
- Keep transport scoping explicit by carrying `subscription_id` on each public entity delivery while preserving Core's established frame tags and payload vocabulary (`entity_snapshot`, `entity_upsert`, `entity_patch`, `entity_remove`; `entity_type`; `snapshot_seq`; `items`/`id`/`entity`/`patch`). Do not introduce the browser adapter's legacy `operation`/`family`/`records` dialect into the daemon contract.
- Define one sanitized session entity schema with the Core entity contract's stable `session_uuid` id plus lifecycle/registry state and only client-safe lifecycle fields needed by first-party clients. Do not expose worker control paths, process environment, host filesystem paths, or raw core implementation types.
- Seed each subscription from one `SessionLifecycleBaseline`; order snapshot rows by stable session id and use its cursor sequence as `snapshot_seq`.
- For later Core changes, compare the authoritative projected row with the subscriber/shared projection: emit `entity_upsert` for a new id, a sparse top-level `entity_patch` for changed fields on an existing id, and `entity_remove` for Core removal. Use each Core change cursor sequence; never fabricate a second counter or reorder changes.
- Add a narrow explicit remove/forget operation through `HubClientApi`/`HubRuntime` only because the current product path exposes no call to CoreDaemon's explicit terminal-session removal and the required production proof must produce `entity_remove`. Do not reinterpret repeated shutdown as removal or automatically delete exited rows.
- Treat a full `entity_snapshot` as the resync signal. When a subscriber queue overflows or its Core cursor reports source-changed/expired/ahead, mark that subscriber stale, discard reliance on queued deltas, and deliver a fresh current snapshot before any later delta. If a snapshot cannot be delivered within the bounded policy, close/fail that subscription with a typed diagnostic rather than silently retaining stale state.
- Use independent bounded state per subscriber. One slow subscriber may coalesce into/resync from its own snapshot or be disconnected, but must neither block the daemon owner loop nor drop/reorder frames for another subscriber.
- Request semantics and a held-open server-push stream change the public protocol contract. Apply the compatibility convention deliberately: add a source-derived required feature for session entity subscriptions, bump the daemon protocol version for the new request/stream semantics, and allocate a conformance fixture revision strictly above every current/published meaning after rechecking main/publication state. Do not reuse revision 15 merely because it is current in this checkout.

## Implementation sequence

1. **Lock and expose Core lifecycle truth.** Update the Core git lock to the merged prerequisite. Add HubRuntime methods that return the Core lifecycle baseline/changes and explicitly remove an eligible terminal session. Keep conversion/projection in hub code and expose no raw Core router to clients.
2. **Build the sanitized projection.** Add a small hub-owned projection type/module only if it materially keeps `daemon_transport.rs` bounded; otherwise keep it near the transport owner. Convert baseline rows deterministically, retain the last projected rows needed for sparse patches, and map Core resync reasons to a fresh authoritative snapshot plus diagnostics.
3. **Extend the internal client facade and admission.** Add subscribe/unsubscribe/remove operations to `HubClientRequest`, `HubClientOperation`, admission checks, response/error mapping, and tests. Reject unsupported entity types and invalid/duplicate subscription ids without hydrating anything.
4. **Wire connection-owned push delivery.** Extend the daemon owner loop and connection handler with registration, bounded outbound delivery, cleanup, and a lifecycle pump. The pump must drive the production CoreDaemon path sufficiently to observe natural exits and must preserve/reroute any terminal egress it drains. Ensure the initial snapshot is enqueued before registration can observe later deltas. Remove subscriptions on explicit unsubscribe, socket EOF, write failure, daemon shutdown, and every error path.
5. **Publish the external contract.** Add serde-only subscription/frame/session-entity DTOs and a held-open stream API in `crates/botster-hub-client/src/lib.rs`; update request labels and exhaustive match sites in `src/main.rs` / `src/local_webrtc.rs` only where compilation or an explicit unsupported diagnostic requires it. Add the feature/compatibility changes, bump the correct revisions, regenerate `crates/botster-hub-client/generated/daemon-protocol.ts`, and keep generated optionality/type checks symmetric with serde.
6. **Add downstream-shaped proof and docs.** Extend `botster-hub-test-support` and its generated package assets/support matrix only as required to advertise and exercise the new public surface. Document subscribe/snapshot/delta ordering, reconnect, cleanup, resync, and the operator-only role of `list_sessions` in `README.md` and `docs/client-protocol.md`.

## Affected surfaces and likely files

- `Cargo.lock` — consume the merged Core lifecycle-source revision.
- `src/runtime.rs` — narrow CoreDaemon baseline/change/remove facades.
- `src/client_api.rs` — internal request/operation/admission/projection-facing contract and unit tests.
- `src/daemon_transport.rs` — production subscription registry, lifecycle pump, ordered/bounded delivery, resync, diagnostics, and disconnect cleanup.
- `src/main.rs`, `src/local_webrtc.rs`, and `src/lib.rs` — exhaustive request labeling/re-exports or explicit unsupported handling only where the new public variants require it.
- `crates/botster-hub-client/src/lib.rs` — external serde DTOs, stream helper, compatibility feature/version/revision, generation and contract tests.
- `crates/botster-hub-client/generated/daemon-protocol.ts` — regenerated authoritative TypeScript artifact.
- `tests/hub_client_api_test.rs` — facade/admission/snapshot/delta mapping tests.
- `tests/hub_daemon_lifecycle_test.rs` — real HubDaemon/CoreDaemon/worker subscription, ordering, concurrency, reconnect, disconnect, and overflow proof.
- `crates/botster-hub-test-support/src/lib.rs` plus `packages/hub-test-support/{daemon-protocol.ts,first-party-client-support-matrix.json,metadata.json,index.d.ts,index.js,test.mjs}` — only if the public conformance/support artifact changes, regenerated from source rather than hand-edited.
- `README.md` and `docs/client-protocol.md` — product path and public protocol semantics.
- This plan artifact should be updated if implementation proves a materially different file boundary or acceptance command.

## Assumptions and unknowns

- Assumption: the merged Core lifecycle source at/after `879f55e` is the accepted prerequisite and no further Core API is needed. If preserving terminal egress while driving lifecycle progress proves impossible through the exported CoreDaemon API, stop and register a new blocking dependency against `botster-core`; do not add a parallel process watcher or silently discard egress.
- Assumption: `session` is the wire entity type and `session_uuid` is its stable entity id field, matching Core's reserved built-in entity contract. The public DTO can retain existing `DaemonSession` list rows separately for operator queries.
- Assumption: a dedicated held-open subscription stream is the smallest safe server-push surface because it avoids ambiguous response/event interleaving on ordinary request connections.
- Unknown until implementation inspects publication state: the next globally unique conformance revision and exact protocol-version value. The rule is fixed (new request/stream semantics require a protocol bump; new fixture/event bytes require a unique conformance bump), but values must be allocated from current source and published artifacts, not this plan's stale guess.
- Unknown: whether the cleanest terminal-egress preservation seam is a small transport-owned pending-egress map or an existing Core/Hub queue that can be reused. The acceptance invariant is fixed: the entity pump cannot steal terminal events.
- Unknown: whether support-package metadata needs a version bump in this implementation or only regenerated source assets. Follow the repository's release convention; do not claim a registry publication unless a separate release step performs it.
- No convention conflict or requested waiver is known. The plan uses existing Core contracts, existing entity vocabulary, the HubRuntime boundary, standard-library bounded delivery, cold-turkey primary synchronization semantics, and repo-owned test wrappers.

## Risks and mitigations

- **Snapshot/delta race:** a Core change between baseline capture and subscriber registration could be missed. Register from the baseline cursor atomically on the daemon owner thread, then replay changes strictly after that cursor before normal pumping.
- **Terminal regression:** lifecycle pumping can consume Core drain output. Preserve and route all terminal egress by client/subscription and retain existing attach ordering; add an adjacent terminal attach/live-output regression.
- **Slow client stalls hub:** socket writes or unbounded queues can block the owner loop. Use bounded per-subscriber queues and the existing write timeout; fail/resnapshot one subscriber independently.
- **Silent stale state after loss:** dropping a delta without resetting the baseline violates the ticket. Track a stale/resync-required state and require the next accepted frame to be a fresh snapshot; disconnect with a typed diagnostic if that cannot be delivered.
- **Mixed generations on reconnect:** reusing a subscription id or queued frames can cross daemon/socket generations. Scope subscriptions to one connection, remove them synchronously on drop, reject duplicates, and require reconnect to create a fresh id/snapshot.
- **Projection drift:** hand-designed session JSON could diverge from serde/generated clients. Serialize one public Rust DTO, derive TypeScript from it, and test symmetric fields, types, and optionality.
- **Compatibility drift:** a new feature constant changes advertised, required, and support-matrix lists. Run workspace-scoped tests and source-derived snapshots, and allocate unique protocol/conformance revisions intentionally.
- **Hub gravity:** generic entity storage or plugin registration would broaden this ticket. Implement only the built-in session projection/subscription seam needed now; leave generic provider infrastructure to Core or a separately approved ticket.
- **Test flakiness:** natural exit and concurrent subscriber tests cross subprocesses. Use `IsolatedHubBuilder`, explicit hub/worker binaries, bounded timeouts, deterministic commands, cleanup guards, and the existing serialized real-daemon lock rather than sleeps as assertions.

## Acceptance checks and downstream proof

1. Client-contract tests through `./test.sh -p botster-hub-client` prove serde round trips for subscribe/unsubscribe/remove and all four entity frame variants; generated TypeScript exactly matches the checked artifact and preserves field names, types, and optionality.
2. Focused HubClientApi tests through `./test.sh --test hub_client_api_test <new-session-entity-filter>` prove admission, deterministic snapshot projection, upsert vs sparse patch vs remove mapping, unsupported-family rejection, and no implicit calls/response bodies for unrelated global state.
3. A real subprocess test through `./test.sh --test hub_daemon_lifecycle_test <new-session-entity-subscription-filter> -- --exact --nocapture` uses the production `HubDaemon -> HubRuntime -> CoreDaemon -> botster-session-worker` path and proves, in order:
   - explicit `session` subscribe returns an authoritative snapshot;
   - spawn publishes `entity_upsert`;
   - a material update and natural process exit publish ordered `entity_patch` frames with strictly increasing Core-derived sequence values;
   - explicit remove publishes `entity_remove`;
   - the client never sends periodic `list_sessions` to converge.
4. A concurrent-subscriber test proves two held-open connections receive equivalent ordered session state independently, while delaying one subscriber neither blocks nor reorders the other.
5. Disconnect/reconnect tests prove server-side subscription count/resources return to baseline after EOF/write failure; reconnect uses a new subscription id, begins with a fresh current snapshot, and receives no queued frame from the prior generation.
6. Overflow tests use a deliberately tiny queue to cover: successful resnapshot recovery, empty current snapshot recovery, snapshot enqueue/write failure, explicit diagnostic/counter evidence, and isolation from a healthy subscriber. No branch may continue with an unannounced delta gap.
7. Adjacent terminal regression proves the lifecycle pump does not consume or reorder terminal attach history/live output and existing detach-on-disconnect behavior remains intact.
8. `./test.sh -p botster-hub-test-support` proves source-derived compatibility/support-matrix and generated npm asset parity if those assets change. The live conformance helper must depend only on `botster-hub-client` and spawn real hub/worker subprocesses.
9. Run the full repository gates after focused tests: `cargo fmt --all -- --check`, `./test.sh`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `git diff --check`. Attribute any failure exactly; a pre-existing or cascade failure is not a blanket waiver.
10. Downstream proof: materialize/compare the generated daemon protocol through the hub test-support package tests. Optionally run botster-web's explicit artifact drift check against this worktree to show the consumer detects the new contract, but do not edit botster-web in this run; record a follow-up ticket for consumer adoption if one does not already exist.
11. Production wiring evidence in the implementation report must trace a real subscriber from the public hub-client helper through the socket handler, HubClientApi/admission, HubRuntime/Core lifecycle baseline and changes, the bounded projection queue, and back to received entity frames. Type existence or generated-file presence alone does not satisfy the ticket.

## Required artifacts and gates

- Keep this plan committed and update it for accepted implementation deviations.
- Implementation evidence must include the Core lock revision, focused and full command outputs, protocol/conformance revision decision, generated artifact diff, runtime path trace, queue/cleanup counters, and exact downstream proof.
- Review and Verify must load the runtime and hub-client overlays, inspect every exit/overflow path, run strict workspace gates, scan committed artifacts for local paths/PII, and reject an implementation that retains list polling or an unwired subscription facade.

## Vault gaps worth capturing

- Capture after implementation if the settled reusable rule is new: a Hub lifecycle projection pump that drives CoreDaemon drain must retain terminal egress while independently advancing entity subscriptions.
- Capture after implementation if the public transport settles a durable pattern for carrying connection-scoped subscription ids on otherwise shared Core entity-frame vocabulary.
- No Plan-time vault write is needed. Existing notes already cover authoritative reconnect snapshots, bounded delivery/resync, connection cleanup, entity-only state synchronization, compatibility/versioning, generated DTO parity, and downstream subprocess proof.
