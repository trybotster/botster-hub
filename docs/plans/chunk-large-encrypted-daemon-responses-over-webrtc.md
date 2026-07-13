# Chunk large encrypted daemon responses over WebRTC

## Context loaded

- Pipeline run `run_1783967464_799454`, Plan step `botster_plan`, ticket `ticket_1783967448_801717`, its required plan gate, and the absence of prior artifacts, reviews, findings, or earlier answers were loaded through Project Pipelines.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]], [[identity]], and [[goals]].
- Botster constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[webrtc e2e encryption now mandatory no plaintext paths]], [[webrtc peer cleanup removes every per peer owner together]], [[late webrtc messages after disconnect must not recreate clients]], [[test script required for rust tests not cargo test]], [[conformance harnesses gate on deterministic invariants not timing]], [[generated typescript dtos must encode serde field optionality]], [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]], and [[adding a hub client feature constant is a three site change]].
- Artifact discipline: [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo path traced: `LocalWebrtcHandler::on_data_channel` receives one encrypted `DaemonRequest`, synchronously routes it through the production daemon `ControlMessage::Request`, serializes and encrypts the returned `DaemonResponse`, and currently calls `DataChannel::send_text` once in `src/local_webrtc.rs`. `tests/hub_daemon_lifecycle_test.rs::LocalWebrtcOfferPeer::encrypted_request` assumes exactly one encrypted response message. The public browser-visible DTO source is `crates/botster-hub-client`; checked TypeScript and npm test-support artifacts publish that contract.
- Cross-repo consumer path traced read-only: `botster-web/src/botster/webrtcDaemonClient.ts` decrypts one `AesGcmEnvelope` per DataChannel message and resolves requests FIFO. It is outside this writable target.
- Corrected human answer `question_1783967593_612932` split delivery into ordered tickets and explicitly requires a cold-turkey protocol replacement. This hub ticket owns the producer/public-contract half. Web adoption ticket `ticket_1783968102_419625` depends on it and owns replacement browser decoding, bounded reassembly, stale/reconnect cleanup, partial-state rejection, and the final packaged 4.7 MB reload proof. There is intentionally no negotiation, legacy fallback, dual decoder, or stagger-safe compatibility scaffolding; the attach-readiness work must resume only after both breaking halves merge.
- Current baseline evidence: `./test.sh botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap -- --nocapture --test-threads=1` passes against the checkout's locked core `b0f8b8e0`. That test uses a small shell-backed response and therefore does not prove the large-message path. The ticket's `db69456c` / approximately 4.7 MB Ghostty evidence remains the motivating downstream reproduction, not a passing hub acceptance substitute.

## Scope

This run changes the Rust hub producer and its published transport contract only.

1. Define one transport-specific, versioned response-chunk frame in `botster-hub-client`. The frame carries a chunk protocol version, collision-safe response message identity, originating request identity, zero-based chunk index, total chunk count, encrypted-envelope byte length, and a payload slice. Payload is a slice of the already encrypted, serialized `AesGcmEnvelope`; daemon response plaintext is never exposed to the chunk layer.
2. Replace encrypted request plaintext with one versioned local-WebRTC request wrapper carrying a client-generated request id plus the existing `DaemonRequest`. Delete the raw-request decoder rather than accepting both shapes. This supplies request identity for chunk correlation without changing the transport-neutral daemon request enum.
3. Replace response framing at the production send boundary. Encrypt and serialize each `DaemonResponse` once, then emit one or more response-chunk frames. Small responses are exactly one chunk through the same framing/reassembly protocol; large responses are multiple chunks. Delete the old direct raw-`AesGcmEnvelope` response send path.
4. Use fixed conservative bounds derived from the actual transport constraints, not optional configuration: every serialized chunk frame must stay below the 64 KiB default remote maximum (with envelope overhead included), and one encrypted response assembly is capped at 16 MiB. The 16 MiB cap covers a 10 MiB raw history budget after AES-GCM/base64/JSON expansion while remaining explicitly bounded. Replace an over-budget response with one bounded encrypted operator-error response, itself carried as one chunk, before sending any bytes from the rejected response.
5. Publish deterministic Rust/TypeScript DTOs and a conformance fixture containing single-chunk, multi-chunk, and over-budget-error scenarios. Increment the conformance fixture revision; expose and checksum the fixture through the Rust and npm `hub-test-support` packages so the downstream web ticket consumes a stable merged contract.
6. Document framing, bounds, correlation, the intentional breaking upgrade, and the ordered two-ticket rollout in `docs/client-protocol.md`.

## Non-scope

- No edits to `botster-web`, browser reassembly, browser timers, duplicate/reorder/missing-chunk policy, reconnect cleanup, or UI state application. Those belong to `ticket_1783968102_419625` after this contract merges.
- No claim that this hub merge alone fixes the packaged-browser reload regression or remains usable by the old browser decoder. The final byte-exact browser and approximately 4.7 MB Ghostty proof belongs to the dependent web ticket after it replaces the decoder.
- No core snapshot decoding, semantic compaction, history-budget reduction, or changes to `botster-core` snapshot/readiness behavior.
- No DataChannel ceiling increase, WebRTC dependency change, new crate/gem/package, optional runtime tuning knob, generic transport abstraction, or unrelated daemon protocol refactor.
- No attach-readiness fixtures/docs owned by `ticket_1783639801_771329`.
- No application-layer reordering of normal DataChannel delivery. The channel remains ordered/reliable; indices detect malformed conformance input and support bounded client assembly, not a hub holdback queue.

## Assumptions and unknowns

- Human-approved assumption: this is an intentionally coordinated breaking upgrade. Hub can merge/publish first to provide the authoritative contract, but old browser clients are temporarily incompatible until `ticket_1783968102_419625` replaces their request encoder and response decoder. No compatibility code should mask that boundary.
- Request identity is client-generated in the replacement request wrapper and echoed on every response chunk. Response/message ids are generated collision-safely per live peer and are not reused while a response is in flight. Single-chunk responses use the same identity and correlation rules as multi-chunk responses.
- The response is encrypted before size selection and chunking, preserving the mandatory encrypted DataChannel invariant. Chunk metadata is transport metadata; final AES-GCM authentication still protects the reassembled encrypted response content.
- The existing handler processes one request/response at a time per peer because it waits synchronously for the daemon reply and then awaits sends. The change should preserve that bounded sequencing rather than introduce concurrent response tasks or queues.
- The implementation must calculate the final serialized chunk-frame size, not assume payload length equals wire length. If a 60 KiB payload target does not leave sufficient JSON metadata headroom, reduce the payload constant until tests prove every frame is below 64 KiB.
- The exact fixture filename/export naming may follow the existing late-attach fixture pattern, but it must remain transport-specific and include no browser timeout/reconnect policy that belongs to the dependent ticket.
- The currently locked core revision is newer than the ticket's motivating `db69456c`, and the existing reload-named test passes because it does not produce the large Ghostty response. Hub acceptance therefore needs an explicit synthetic oversized response over the real encrypted WebRTC path; no sibling-worktree result is final evidence.
- Unknown for implementation to verify: whether the WebRTC crate exposes the negotiated remote `max-message-size` through the current public API. The minimum required behavior is the conservative default-safe frame bound; do not grow scope by patching the dependency merely to expose a larger negotiated value.

## Affected Botster layers and files

- Rust hub transport/data-plane adapter:
  - `src/local_webrtc.rs` — replacement request decoding, response encryption, bounded single/multi-chunk sender, over-budget error handling, and cleanup-compatible per-peer ids.
- Public client protocol:
  - `crates/botster-hub-client/src/lib.rs` — request wrapper, chunk frame DTO, and conformance revision.
  - `crates/botster-hub-client/src/typescript.rs` and `crates/botster-hub-client/generated/daemon-protocol.ts` — serde-accurate browser contract and checked generated artifact.
- Shared conformance and published artifacts:
  - `crates/botster-hub-test-support/src/lib.rs` and `crates/botster-hub-test-support/examples/node_package_assets.rs` — deterministic framing fixture/helper and Node asset emission.
  - `packages/hub-test-support/` fixture JSON, `metadata.json`, `index.js`, `index.d.ts`, `scripts/sync-assets.mjs`, and `test.mjs` — checked npm copy, checksums, exports, and consumer assertions.
- Production-path and protocol verification:
  - `tests/hub_daemon_lifecycle_test.rs` — replace the Rust WebRTC offerer's raw request/response codec with request ids and response-chunk reassembly, then prove small one-chunk traffic and a synthetic response larger than 256 KiB cross the real encrypted DataChannel byte-exactly.
  - `docs/client-protocol.md` — compatibility and rollout contract.
  - `docs/plans/chunk-large-encrypted-daemon-responses-over-webrtc.md` — this handoff artifact.

## Implementation sequence and production wiring

1. Add and round-trip the replacement public DTOs, generated TypeScript, and fixture revision. Do not add feature negotiation or touch daemon-wide feature lists.
2. Add pure framing helpers with exact serialized-size enforcement, checked arithmetic for chunk counts/total bytes, the 16 MiB cap, and deterministic ids injectable in unit tests.
3. Decode only the replacement request wrapper, preserve its request id through the existing owner-thread `ControlMessage::Request` call, and replace the one raw-envelope `send_text` call with one-or-more chunk sends. Do not change the daemon response producer or core semantics.
4. Extend the real Rust WebRTC harness. The required runtime proof is the actual production entry path: browser-shaped encrypted request wrapper -> `LocalWebrtcHandler::on_data_channel` -> `ControlMessage::Request` -> daemon/core response -> encryption -> framing helper -> repeated real `DataChannel::send_text` -> harness reassembly/decryption. Pure helper tests are necessary but not sufficient.
5. Publish the conformance fixture and docs after behavior is fixed, then run drift/checksum/package consumer guards.

## Risks

- **Coordinated breaking window:** the merged hub producer is incompatible with the old browser request/response codec until the dependent web ticket lands. Mitigation: document and track the ordered dependency explicitly; do not add dual paths that prolong ambiguity.
- **Accidental compatibility scaffolding:** retaining the raw request decoder or raw response sender would create two protocols and undermine the cold-turkey decision. Mitigation: tests reject old shapes and source inspection proves the deprecated paths are deleted.
- **Oversized frame despite chunking:** JSON quoting, ids, and multi-digit indices add overhead. Mitigation: assert final serialized size for every emitted frame and keep headroom below 64 KiB.
- **Memory amplification/overflow:** a malicious or accidental huge response could allocate serialized, encrypted, and chunk copies. Mitigation: checked lengths, 16 MiB encrypted-envelope cap, slices rather than full per-chunk clones where the API permits, and fail before send.
- **Correlation drift:** FIFO pending requests plus asynchronous chunks can apply a response to the wrong request. Mitigation: replacement request ids echoed on every chunk, one active response sequence per peer, unique response ids, and conformance assertions for exact identity.
- **Partial terminal state:** a failed send after some chunks must never be represented as a complete response. Hub cannot apply browser state, but it must stop the response, close/clean the peer through the existing lifecycle path, and never send a completion marker for incomplete output. Browser rejection/cleanup remains the web ticket.
- **Stale peer state:** request/message ids surviving close could contaminate a new grant. Mitigation: store them in `LocalWebrtcPeerState`, which dies with peer cleanup; do not place response state in global `LocalWebrtcTransport` state.
- **Test false positive:** the current reload-named test passes without a large payload. Mitigation: assert pre-chunk encrypted size exceeds 256 KiB, chunk count exceeds one, each frame is below 64 KiB, and decrypted bytes equal the original response.
- **Fixture drift:** Rust behavior, generated TypeScript, crate fixture, npm copy, and docs can diverge. Mitigation: one Rust source of fixture truth plus existing generation/checksum/drift guards and workspace-wide tests.

## Acceptance checks and tests

Required deterministic checks for this hub ticket:

1. Unit/DTO tests prove:
   - the replacement request wrapper and response chunk frame round-trip through serde and generated TypeScript;
   - deprecated raw request plaintext and raw encrypted response frames are no longer accepted/emitted;
   - small encrypted responses use exactly one chunk through the same protocol as large responses;
   - an encrypted response over 256 KiB yields contiguous indexed frames with stable request/message ids, declared total bytes/count, no duplicates, and every serialized frame below 64 KiB;
   - exactly-at-boundary, one-byte-over-boundary, malformed arithmetic, and over-16-MiB cases fail boundedly;
   - over-16-MiB responses send one single-chunk encrypted operator error tied to the original request id before any rejected-response payload;
   - send failure mid-response triggers existing peer cleanup and does not emit a terminal/completion frame.
2. Real WebRTC integration in `tests/hub_daemon_lifecycle_test.rs` proves a synthetic encrypted daemon response larger than 256 KiB traverses the production adapter and reassembles/decrypts byte-exactly in the Rust browser-shaped peer. Assert measured pre-chunk size and per-frame size, not only final response kind.
3. Repeat the focused oversized real-WebRTC test several times under the existing `REAL_DAEMON_TEST_LOCK` and single test thread; exact byte/count invariants are gates, elapsed time is observation only.
4. Update and run existing signaling, grant, small request/response, reload-bootstrap, and peer-close subscription cleanup tests against the replacement codec; no old-codec compatibility assertion remains.
5. Run generated/public artifact guards and package consumption:
   - `./test.sh -p botster-hub-client`
   - `./test.sh -p botster-hub-test-support`
   - `node packages/hub-test-support/test.mjs`
6. Run repo-standard quality gates:
   - `cargo fmt --check`
   - repo-enforced strict Clippy command discovered from `Cargo.toml`
   - `./test.sh` for the full supported workspace suite; do not dismiss an exact failure as a `REAL_DAEMON_TEST_LOCK` flake without isolated unrelated evidence.
7. Scan the complete branch diff for secrets, absolute local paths, usernames, and other PII.

Deferred acceptance owned by `ticket_1783968102_419625`, not evidence for this hub gate:

- browser replaces its request encoder and response decoder, reassembles one- and multi-chunk responses byte-exactly with bounded memory, and rejects missing, duplicate, reordered, stale, oversized, timed-out, reconnect-interrupted, or wrong-attachment assemblies without partial terminal state;
- the real packaged approximately 4.7 MB Ghostty reload regression passes repeatedly against the merged hub contract and required core revision.

## Project Pipelines and vault checklist evidence

- Target/worktree: this plan applies only to the pipeline-provided `botster-hub` ticket worktree and explicit target. It does not authorize edits in a sibling checkout.
- Gate artifact: this committed plan plus the attached Project Pipelines artifact is the Plan Review handoff. The next step should compare implementation scope against the human-approved two-ticket split.
- Convention conflict check: no unresolved convention conflict. The corrected human decision applies [[cold turkey migrations eliminate dual code paths and version suffixes]]. The older [[Snapshots delivered as atomic WebRTC messages, not chunked]] decision is contradicted by current measured transport limits and should be superseded, not silently ignored. The replacement remains transport-specific, encrypted, bounded, and ordered, with one framing path for small and large responses.
- Verification evidence at Plan time: repo/source/vault inspection listed above; focused current-baseline reload test passed but was classified as small-message-only evidence.
- Checklist persistence: `project_pipelines_create_vault_checklist` returned `plugin worker invoke timeout`. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this section and the plan gate carry notes read, conflict result, verification evidence, and capture disposition.
- Durable capture disposition: update/supersede [[Snapshots delivered as atomic WebRTC messages, not chunked]] after implementation proves the new constraint, and capture a new atomic note that encrypted daemon responses use one bounded WebRTC chunk protocol for both small and large payloads. Capture should happen from implementation evidence rather than this unverified plan; no vault write is made during Plan.

## Vault gaps worth capturing

- The current atomic-snapshot note is now misleading for this production hub path: SCTP fragmentation does not make application chunking sufficient when the actual library/browser limits reject a multi-megabyte DataChannel message. This warrants an explicit superseding transport decision after proof lands.
- The coordinated cold-turkey replacement of both producer and consumer is itself durable knowledge: protocol ambiguity is a greater risk here than a temporary ordered-ticket incompatibility window. Capture it only after both tickets prove the new path.
- A conformance fixture should define producer framing and bounds while leaving browser timeout/reconnect policy to the consumer ticket. If that boundary proves useful during web adoption, capture it as a cross-repo conformance pattern.
