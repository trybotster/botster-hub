# Hub Decomposition 4a: Split WebRTC By Channel Role And Retire local_webrtc.rs

## Revision History

Revision 3, Plan visit 3, answering `review_1787999655_409032`, verdict `blocked`, and the human decision in `question_1787999576_734551`.

The human overruled revision 2's tracked-not-blocking call, and the reasoning is stronger than mine: the failing proof exercises **the same WebRTC delivery path this ticket moves**, so a base-versus-HEAD failure comparison plus isolated HEAD success cannot exclude a new load-only regression. Revision 2 argued from "the flake is pre-existing"; the correct frame is "the flake sits inside the blast radius of this move". The differential protocol is withdrawn as final acceptance evidence.

Changes in revision 3:

- The blocking dependency `dependency_1787999625_716785` on `ticket_1787999248_674913` is recorded. That ticket is now **closed**; its repair merged to `main`.
- **Base re-pinned from `38d140c` to `ddb2de9`.** This branch was rebased onto the repaired `main`. The repair landed in `60aa4c4`, `dbaee44`, `c09f4d2`, `4390758`, and `ddb2de9`.
- Acceptance check 7 is restored to an **absolute full-suite gate**. The differential `BASE_FAIL`/`HEAD_FAIL` protocol is deleted, not merely demoted.
- Every base fact was re-measured on `ddb2de9` rather than carried over. See "Context Loaded".
- "Baseline Suite State" is rewritten to record the repair and the new expectation, replacing the disposition argument that the human decision closed.

Revision 2, Plan visit 2, run step `run_step_1787999007_348502`. It answers `review_1787998985_537393`, verdict `changes_required`:

- `finding_1787998985_266740` (high, product): stale WebRTC 0.20 premise. Fixed. I re-verified `Cargo.toml:38` and `Cargo.lock`: the base resolves `webrtc 0.21.0-beta.2`. Revision 1 asserted 0.20 from charter prose without reading the manifest, which was my error. Non-scope item 9, the notes list, and the risks now state the verified 0.21.0-beta.2 base, and the plan states that the single `botster-daemon` channel is preserved because the ticket requires it, not because the crate prevents late channels.
- `finding_1787998985_376877` (high, product): the late-message matrix grouped ownership-creating requests under one generic `DaemonRequest` row. Fixed. I traced `run_data_channel` and `cleanup_once` in `src/local_webrtc.rs` and the `LocalWebrtcPeerClosed` handler in `src/daemon_transport.rs`. The matrix now carries explicit rows for `Attach`, `Detach`, `SubscribeEntities`, `UnsubscribeEntities`, `SubscribeEvents`, `UnsubscribeEvents`, and `Spawn`, each with its owner tag, terminal rejection, and `PeerClosed` race sweep.
- `finding_1787998985_197852` (high, product): the locked baseline suite is not green. Accepted. `webrtc_terminal_output_is_byte_exact` is recorded as a measured base-red, owner ticket `ticket_1787999248_674913` is registered against `botster-hub`, and acceptance check 7 is replaced by a differential base-versus-HEAD protocol that can distinguish this move from the pre-existing load failure. See "Baseline Suite State".
- `finding_1787998985_198296` (high, product): the commit plan weakened the ticket. Fixed. Acceptance check 18 now requires one compiling move-only commit, matching the ticket acceptance text.
- `finding_1787998986_615689` (medium, product): the guard census said six. Fixed. The base holds nine matching expressions across seven files. "Context Loaded" now carries the exact table and acceptance check 11 accounts for each expression.
- `finding_1787998986_147916` (low, process): Plan step completion evidence was empty although the gate evidence and `artifact.added` event exist. Revision 2 reuses `artifact_1787998020_136225` and `checklist_1787997782_893725`, and passes the five required fields to `request_step_advance` as well as to `submit_gate`.

## Target Repository And Target Id

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`).
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1787894421_128594`. Run: `run_1787997552_597206`. Run step: `run_step_1787997552_126164`.
- Base ref: `main`. Base commit: `ddb2de9cdc11a2e3a050e477cf396685686887f2`, the repaired baseline. The branch was rebased onto it in Plan visit 3. The former base `38d140c` is superseded.
- The target repository comes from the ticket `target_id` resolved through `list_spawn_targets`. It does not come from the process working directory.
- Merge policy: direct to `main`.

## Repository Playbook Loaded

- [[botster-hub-playbook]] -- the exact repository ownership charter for `botster-hub`.

## Other Role And Surface Playbooks And Atomic Notes Loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Class overlay, required because this ticket touches WebRTC peer lifecycle and channel teardown:

- [[botster runtime teardown lenses]]

Architecture and ownership notes:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[daemon transport extraction moves ownership before deleting the facade]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[botster data plane bypasses the hub through session and client actors]]

Transport topology notes that bound the non-scope of this ticket:

- [[botster subscriptions use dedicated ordered DataChannels]] -- the target topology, not implemented here.
- [[the browser creates each subscription DataChannel after Hub reserves its label]] -- the target creation order, not implemented here.
- [[webrtc 0 21 restores post handshake DataChannel creation in Hub]] -- current. The base resolves `webrtc 0.21.0-beta.2`, so post-handshake channel creation is available.
- [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]] -- historical, superseded by the 0.21 note above. Recorded so no reader treats the 0.20 limit as current. This ticket keeps the single `botster-daemon` channel because the ticket requires it, not because the crate forbids a second one.

Source-guard notes, required because this move retargets nine guard expressions across seven files:

- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[code moves need paired absence and presence source guards]]
- [[region bounded source guards need a required symbol anchor]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[exact Rust test ablations require a one test baseline]]

WebRTC behavior notes that name the oracles this move must preserve:

- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[Fault-injected WebRTC close requires a daemon started with the inject env]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[Protocol 7 gates WebRTC daemon events on close-event Hello negotiation]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[rejected channel isolation needs a surviving channel positive control]]
- [[Fair host-control writing selects already-admitted frames]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]

Gate and hygiene notes:

- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[colon worktree paths break cargo dyld library paths]]
- [[express scope limits as invariants not closed enumerations]]

Not loaded, with reason:

- [[project-pipelines-playbook]]: no Project Pipelines package or plugin path is in scope.
- Other repository charters: no other repository changes in this ticket.

## Context Loaded

- Project capture: `/Users/jasonconigliari/knowledge/ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md`, vault commit `8ef01f56`. This ticket is migration step 5 for the WebRTC half, plus step 6 for `local_webrtc.rs`.
- Project checklist `checklist_1787894551_481961`, "Wake-driven data-plane architecture adherence".
- Closed dependency `ticket_1787894419_699597` (Hub decomposition 3). Its landed commit is `667648a`, which created `src/transport/shared` and `src/transport/unix` and rebuilt the WebRTC adapter over the shared slot.
- Prior art in the target repository: `docs/plans/hub-decomposition-1-*.md`, `docs/plans/hub-decomposition-2-*.md`, `docs/plans/hub-decomposition-3-*.md` and their reports under `docs/reports/`. `docs/plans/` is the repository-confirmed plan destination.
- Measured base state, re-measured on the repaired base `ddb2de9` in Plan visit 3 rather than carried over from `38d140c`:
  - `src/local_webrtc.rs` is 7752 lines. Production ends at line 2437. `mod tests` spans lines 2438 to 7752 as one flat module with 54 `#[test]` functions and a large shared harness (`PeerHarness`, `TestOfferPeer`, `OwnedWorkerIdentity`, process-tree helpers).
  - `src/webrtc_terminal_adapter.rs` is 600 lines and owns `WebRtcTerminalAdapter`, `WebRtcTerminalAdapterHandle`, `WebRtcTerminalAdapterInner`, and `WebRtcConnectionMux`.
  - `src/lib.rs` exports `pub use local_webrtc::{LocalWebrtcError, LocalWebrtcTransport};` at line 167. The module itself is `pub(crate)`.
  - Source-guard census at base: **nine matching expressions across seven files**. Every row must be retargeted or explicitly declared unchanged.

    | # | File | Line | Expression | Asserts | Post-move target |
    |---|------|------|-----------|---------|------------------|
    | 1 | `src/lib.rs` | 1060 | `("src/local_webrtc.rs", include_str!("local_webrtc.rs"))` | guard-list membership for the forbidden-construct scan | replaced by one entry per new `src/transport/webrtc/**` file |
    | 2 | `src/host_control_fair_write.rs` | 163 | `include_str!("local_webrtc.rs")` | the WebRTC fair-write call site passes live `entity_ready` | `transport/webrtc/control_channel.rs` |
    | 3 | `src/local_webrtc.rs` | 6389 | `include_str!("local_webrtc.rs")` | the two ultimate-close-failure oracles survive | `transport/webrtc/peer.rs`, self-scan |
    | 4 | `src/transport/shared.rs` | 85 | `include_str!("../webrtc_terminal_adapter.rs")` | `WebRtcConnectionMux` stays out of shared transport | `include_str!("webrtc/adapter.rs")` |
    | 5 | `src/webrtc_terminal_adapter.rs` | 591 | `include_str!("webrtc_terminal_adapter.rs")` | adapter self-scan | `transport/webrtc/adapter.rs`, self-scan |
    | 6 | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | 92 | `hub_source("src/local_webrtc.rs")` | production one-shot claim in `on_data_channel`; region-bounded | `hub_source("src/transport/webrtc/peer.rs")` |
    | 7 | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | 429 | `hub_source("src/local_webrtc.rs")` | exact multi-line deferred-egress needle from `run_data_channel` | `hub_source("src/transport/webrtc/control_channel.rs")` |
    | 8 | `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | 771 | `include_str!("../../src/webrtc_terminal_adapter.rs")` | adapter production source is content blind | `include_str!("../../src/transport/webrtc/adapter.rs")` |
    | 9 | `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | 772 | `include_str!("webrtc_terminal_adapter.rs")` | the sibling integration test file is content blind | unchanged; it scans a test file, not a moved source |

  - Scanner state at base, measured with the `scan_production_source` algorithm from `src/lib.rs:888`: `src/local_webrtc.rs` `skip_open_at_eof = false`, `src/webrtc_terminal_adapter.rs` `skip_open_at_eof = false`, `src/host_control_fair_write.rs` `skip_open_at_eof = false`. No base needle repair is required. The split can still introduce a leak in any new file whose final `#[cfg(test)]` block ends with unbalanced braces inside string needles.
  - `src/lib.rs` names `local_webrtc` in three test sites: the `HubCrateExport` row at line 546, the crate-root module list at line 770, and the internal-visibility list at line 820.
  - `src/daemon_transport.rs` holds 48 `local_webrtc` references, all import or call sites, plus `persist_local_webrtc_terminal_record` and `detach_local_webrtc_subscriptions`, which are control dispatch and stay.
- Worktree hygiene at Plan time: `git status --porcelain` is empty, tracked `.gitignore` has 5 lines, and the worktree path contains no `:`.

## Scope And Non-Scope

### In scope

1. Create `src/transport/webrtc.rs` as the module root and create the six role files named by the ticket:
   - `src/transport/webrtc/peer.rs`
   - `src/transport/webrtc/signaling.rs`
   - `src/transport/webrtc/control_channel.rs`
   - `src/transport/webrtc/subscription_channel.rs`
   - `src/transport/webrtc/delivery.rs`
   - `src/transport/webrtc/adapter.rs`
2. Move every production symbol out of `src/local_webrtc.rs` and `src/webrtc_terminal_adapter.rs` into those files by state machine and channel role, using the allocation in "Affected Surfaces And Files".
3. Move every test out of the two source files into the role file that owns the symbol under test, and move the shared `cfg(test)` harness into `src/transport/webrtc.rs`.
4. Delete `src/local_webrtc.rs` and `src/webrtc_terminal_adapter.rs`. Leave no forwarding facade and no re-export shim inside either path.
5. Retarget all nine guard expressions listed under "Context Loaded" to the new owner files, and extend the `src/lib.rs` guard list with every new `src/transport/webrtc/**` file.
6. Update the `src/lib.rs` `architecture_summary` rows and module lists: remove the `local_webrtc` crate-root row and its two list entries, and keep the existing `transport` row.
7. Update `src/lib.rs` so the public re-export path `botster_hub::LocalWebrtcError` and `botster_hub::LocalWebrtcTransport` stays identical, now sourced from `crate::transport::webrtc`.
8. Update import paths in `src/daemon_transport.rs`, `src/daemon.rs`, `src/daemon/error.rs`, `src/client_api_dto/response.rs`, `src/main.rs`, `src/local_runtime_process.rs`, `src/host_control_fair_write.rs`, `src/admission/grants.rs`, `src/local_webrtc_smoke.rs`, and the affected `tests/hub_daemon_lifecycle` files.

### Explicitly out of scope

1. The daemon owner loop and control dispatch stay in `src/daemon_transport.rs`. `persist_local_webrtc_terminal_record` and `detach_local_webrtc_subscriptions` do not move.
2. Grants, labels, peer generations, and budgets stay in `src/admission/`. No admission symbol moves into `src/transport/webrtc/**`.
3. The wake-driven data plane, the Core waking adapter contract, and the targeted pump are not implemented here.
4. Dedicated per-subscription DataChannels are not implemented. The single `botster-daemon` DataChannel remains the unchanged runtime path, and Hub keeps rejecting every second channel.
5. No protocol, DTO, serde name, encryption, framing, chunking, limit, or scheduling change.
6. No Core pin change in `Cargo.toml` or `Cargo.lock`.
7. No public export change. No `botster-hub-client`, `botster-web`, or `botster-tui` change.
8. No behavior repair to any bug found while moving. Any such finding becomes a separate ticket.
9. No `webrtc` crate version change. The base resolves `webrtc 0.21.0-beta.2` at `Cargo.toml:38` and in `Cargo.lock`, verified in this worktree. The single `botster-daemon` DataChannel is preserved because the ticket requires it, not because the crate prevents post-handshake channel creation.

## Repository Ownership Boundaries And Cross-Repository Dependencies

- `botster-hub` owns every changed file. This ticket changes no other repository.
- Hub keeps concrete WebRTC transport as Hub modules, per [[concrete terminal transports stay in hub until a second host needs them]]. This ticket creates no transport crate.
- The Core boundary is unchanged. `src/transport/webrtc/adapter.rs` keeps the same `TerminalAdapter` implementation over the shared slot from decomposition 3, and Hub stays content blind.
- Admission policy stays in `src/admission/`. `src/transport/webrtc/**` receives an accepted peer configuration and a session key. It does not gain grant issue, origin policy, label reservation, or budget code.
- Subscription route ownership stays in `src/subscription/`. `ClosedEventRoute`, `ClosedHandle`, suppression, and `DaemonEvent` construction do not move into the transport tree.
- Cross-repository dependencies: none. The closed dependency `ticket_1787894419_699597` is already merged as `667648a` and is the base for this work.
- Blocking dependency, now satisfied: `dependency_1787999625_716785` binds this ticket to `ticket_1787999248_674913`, the `webrtc_terminal_output_is_byte_exact` baseline repair against the same `botster-hub` target. That ticket is closed and its repair is merged, so the dependency no longer blocks. See "Baseline Suite State".
- Downstream cost: zero. The two public names keep their crate-root paths, so no generic client rebuild is required. If any public export or DTO changes, the ticket has left move-only scope and the Implementer must stop and re-plan.

## Assumptions And Unknowns

1. **Assumption.** `src/transport/webrtc.rs` is required as the module root, in the same shape as the existing `src/transport/unix.rs`. It is not one of the six role files. It owns module declarations, crate-internal re-exports, the WebRTC error taxonomy (`LocalWebrtcError`, `LocalWebrtcResult`, and `impl From<crate::admission::grants::GrantAdmissionError> for LocalWebrtcError`), and the shared `cfg(test)` harness. The ticket names six role files and does not name an error owner. Placing the error taxonomy in one role file would make that role the de facto owner of the other five.
2. **Assumption.** The shared `cfg(test)` harness (`PeerHarness`, `TestOfferPeer`, `TestOfferHandler`, `OwnedWorkerIdentity`, `session_worker_identities`, `worker_owned_process_tree`, `process_is_alive`, `live_pids_in_process_group`, `unique_test_data_dir`, `start_test_daemon_with_event_queue`, `wait_until`, `soft_wait_until`, and the two test locks) moves once into `src/transport/webrtc.rs` under `#[cfg(test)] mod test_support`, and the role files import it. Duplicating the harness would change proof counts, and putting it in `peer.rs` would make `peer.rs` the owner of every sibling's tests.
3. **Assumption.** `subscription_channel.rs` owns the current second-channel path, which today is rejection, not acceptance. Hub reserves no label yet, so "subscription-channel acceptance mechanics" currently means the extra-channel reject path, its close marker, and its test observation seam. This file is live production code on `on_data_channel`, not scaffolding.
4. **Assumption.** `claim_data_channel` stays a method on `LocalWebrtcPeerState` in `peer.rs`, and `on_data_channel` stays in `peer.rs`. Only the reject path and its label, marker, and observation mechanics move to `subscription_channel.rs`. Moving the one-shot claim would change the exact needle `let claimed = self.peer_state.claim_data_channel();` that `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:92` asserts, which the ticket forbids.
5. **Assumption.** A private helper used by exactly one role file moves to that file. A private helper used by two or more role files moves to the file that owns its state machine and becomes `pub(crate)` or `pub(super)` there. No helper is duplicated.
6. **Assumption.** `src/local_webrtc_smoke.rs` is a `main.rs`-only module and is not renamed. It only needs its import paths updated. The ticket names `local_webrtc.rs` for deletion, not the smoke module.
7. **Assumption.** Module paths of moved tests change, and leaf test names do not. `cargo test --exact` filters in scripts or docs that use full module paths must be updated with the rename map, per vault gap 6 below; no vault note covers this yet.
8. **Unknown.** The exact per-test allocation of the 54 `#[test]` functions is resolved during implementation by the rule in assumption 5. The Implementer must record the final allocation table in the implementation report.
9. **Unknown.** Whether any moved `#[cfg(test)]` block leaves a new file in scanner skip mode at end of file. Base is clean for both files. Acceptance check 14 measures every new file after the move.
10. **Unknown.** Whether `src/local_webrtc.rs` production code contains a symbol that belongs to none of the six roles. If the Implementer finds one, they must not invent a seventh role file. They must record it, place it with its state-machine owner, and explain the placement in the report.

## Affected Surfaces And Files

### New files and their owned state machines

`src/transport/webrtc.rs` -- module root:

- `pub(crate) mod` declarations for the six role files.
- `LocalWebrtcError`, `LocalWebrtcResult`, `impl fmt::Display for LocalWebrtcError`, `impl Error for LocalWebrtcError`, `impl From<GrantAdmissionError> for LocalWebrtcError`.
- Crate-internal re-exports so `src/lib.rs` can keep `pub use`.
- `#[cfg(test)] mod test_support` with the shared harness from assumption 2.

`src/transport/webrtc/peer.rs` -- peer lifecycle and ownership:

- `LocalWebrtcTransport` and its fields `peers`, `peer_states`, `stale_close_peers`, `runtime`, and the `cfg(test)` census fields.
- `stop_all`, `remove_peer`, `take_remove_result`, `has_live_peer`, `park_runtime_if_idle`, `fail_closed_drop_dedicated_runtime`, `close_peer_on_runtime`, `runtime`, `active_peer_count`, `has_dedicated_runtime`, `stale_close_peer_count`, `close_completion_count_for`, `dedicated_runtime_worker_threads`, `peer_state_count`, and the `cfg(test)` force and inject hooks.
- `ClosePeerOutcome`, `PeerRemoveResult`.
- `LocalWebrtcPeerState`, `LocalWebrtcTerminalState`, `LocalWebrtcTerminalCause` and its `Display`, `LocalWebrtcCleanupDisposition`, `LocalWebrtcChannelTerminalSignal`, `LocalWebrtcSenderTerminalRecord`, `LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE`, `LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES`.
- `LOCAL_WEBRTC_PEER_CLOSE_BOUND`, `LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE`, `TEST_CLOSE_LOCAL_WEBRTC_OPERATION_ENV`, `TEST_DISABLE_ONE_SHOT_CLAIM_ENV`.
- `LocalWebrtcHandler` and its `PeerConnectionEventHandler` impl, including `on_data_channel`.
- `webrtc_runtime`, `local_webrtc_peer_connection_state`, `SharedEventPlane`.

`src/transport/webrtc/signaling.rs` -- offer and answer state machine:

- `signal`, `issue_bootstrap`, `answer_offer`, `LocalWebrtcAnswer`, `WEBRTC_SIGNAL_OPERATION`, the ICE gather-complete plumbing, `random_token`, `hex`.

`src/transport/webrtc/control_channel.rs` -- the current single `botster-daemon` channel loop and its scheduling:

- `LocalWebrtcDataChannel` trait and its blanket impl.
- `run_data_channel`, `close_data_channel`, `poll_data_channel_or_peer_terminal`, `send_text_or_peer_terminal`.
- `LocalWebrtcInbound`, `DataChannelPlaintext`, `decrypt_data_channel_plaintext`, `apply_data_channel_event`.
- `PendingLocalWebrtcRequest`, `pop_pending_request`, `LOCAL_WEBRTC_PENDING_REQUESTS`, `queued_request_overflow_response`, `local_webrtc_request_operation`, `response_with_diagnostic`.
- `flush_ready_webrtc_host_control`, `webrtc_control_request_ready`, `host_event_ready`, `take_host_event`, `flush_webrtc_host_events`, `flush_webrtc_adapter_frames`, `send_response_frames`.
- `LocalWebrtcFlowControl`, `LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW`, `LOCAL_WEBRTC_BUFFERED_AMOUNT_HIGH`, `LOCAL_WEBRTC_EVENT_PROBE`.

`src/transport/webrtc/subscription_channel.rs` -- second-channel admission mechanics:

- `reject_extra_data_channel`, `observe_rejected_data_channel_for_test`, `EXTRA_DATA_CHANNEL_LABEL`, `TEST_EXTRA_CHANNEL_CLOSE_MARKER_ENV`, `TEST_EXTRA_CHANNEL_OBSERVATION_ENV`.
- `LocalWebrtcAttachedSubscription`, `LocalWebrtcAttachedSubscriptionChange`, `local_webrtc_attach_change_for_response`, `entity_frame_subscription_id`.

`src/transport/webrtc/delivery.rs` -- sealing, framing, and chunking:

- `encrypt_daemon_response`, `encrypt_daemon_entity_frame`, `framed_daemon_response`, `framed_daemon_hello_ack`, `framed_daemon_event`, `framed_daemon_terminal_frame`, `framed_daemon_entity_frame`, `frame_encrypted_daemon_delivery`.
- `LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES`, `LocalWebrtcSendFailure` and its `Display`.

`src/transport/webrtc/adapter.rs` -- the whole current `src/webrtc_terminal_adapter.rs`:

- `WebRtcTerminalAdapter`, `WebRtcTerminalAdapterHandle`, `WebRtcTerminalAdapterInner`, `WebRtcConnectionMux`, `WebRtcMuxInner`, the `TerminalAdapter` impl, the `ClosedHandle` impl, `Drop`, `Default`.

### Deleted files

- `src/local_webrtc.rs`
- `src/webrtc_terminal_adapter.rs`

### Changed files

- `src/lib.rs`: module list, `pub use` source path, `architecture_summary` rows, guard-list entries, module-name lists.
- `src/transport.rs`: add `pub(crate) mod webrtc;`.
- `src/transport/shared.rs`: retarget `include_str!("../webrtc_terminal_adapter.rs")` to `include_str!("webrtc/adapter.rs")` at the correct relative path.
- `src/host_control_fair_write.rs`: retarget `include_str!("local_webrtc.rs")` to the control-channel file.
- `src/daemon_transport.rs`, `src/daemon.rs`, `src/daemon/error.rs`, `src/client_api_dto/response.rs`, `src/main.rs`, `src/local_runtime_process.rs`, `src/admission/grants.rs`, `src/local_webrtc_smoke.rs`: import paths only.
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`: retarget both `hub_source("src/local_webrtc.rs")` calls.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`: retarget `include_str!("../../src/webrtc_terminal_adapter.rs")`.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs`, `webrtc_fixtures.rs`, `event_plane_saturation.rs`, `cli.rs`, `shutdown.rs`: import paths only, where any exist.

## Risks

1. **Silent guard blinding.** A `!contains` guard whose file list still points at a deleted path fails loudly, but a region-bounded guard whose subject moved out of the region goes green and blind. See vault gap 5 below; no vault note covers this failure mode yet. The `src/local_webrtc.rs:6389` self-scan and the `subscription_ownership_baseline.rs:92` region split on `async fn on_data_channel` are both exposed. Mitigation: acceptance checks 11, 12, and 13.
2. **Exact-needle breakage.** `subscription_ownership_baseline.rs:429` asserts a multi-line, exactly indented needle from `run_data_channel`. Any reindentation during the move silently changes behavior-preservation evidence into a failure, or worse, a rewritten needle hides a real edit. Mitigation: acceptance check 4 forbids any needle text edit; only the `hub_source` path may change.
3. **Test-name drift.** Moving 54 tests plus helpers across seven files can rename, drop, or duplicate a proof. Mitigation: acceptance check 8, a base-to-HEAD leaf-name multiset comparison with an explicit module-path rename map.
4. **Scanner skip-mode leak.** A moved `#[cfg(test)]` block whose needle strings hold unbalanced braces can leave a new file in skip mode at end of file, which disables every forbidden-construct guard on that file's tail. Base is clean, so a leak would be newly introduced. Mitigation: acceptance checks 14 and 15.
5. **Cyclic module dependencies.** `peer.rs` calls into `subscription_channel.rs` and `control_channel.rs`, while both call back into `LocalWebrtcPeerState` in `peer.rs`. Rust allows this inside one crate, but a careless move can pull peer state into the control channel. Mitigation: acceptance check 3 asserts each state machine's owning type is declared in exactly one file.
6. **Ownership creep into transport.** A move can drag a grant, label, or route symbol into `src/transport/webrtc/**`. Mitigation: acceptance check 5 extends the existing shared-transport forbidden-identifier guard to the WebRTC tree.
7. **Very large diff hides a semantic edit.** The move is roughly 8300 lines. Mitigation: acceptance check 18 requires `git diff --color-moved=dimmed-zebra` and a move-only commit shape.
8. **A load-only regression inside the moved path.** This is the risk the human decision named when it blocked the ticket: a fault that appears only under full-suite concurrency, in the same WebRTC delivery path this move touches. It is now testable, because the base is green after `ticket_1787999248_674913`. Mitigation: the absolute full-suite gate in acceptance check 7, with `webrtc_terminal_output_is_byte_exact` explicitly required to pass under load rather than only in isolation.

## Runtime-Teardown Class

`teardown_class_applies`: **yes**. The ticket moves WebRTC peer lifecycle, the peer close bound, fail-closed sibling sacrifice, the channel terminal-cause taxonomy, and the cleanup-once path. [[botster runtime teardown lenses]] applies whenever peer lifecycle code is touched, including a move.

This ticket changes no teardown behavior. Every answer below states the behavior that must survive the move unchanged, and names the oracle that proves it survived.

`teardown_isolation`: One failed peer's ownership set is its `LocalWebrtcPeerState`, its data-channel task, its attached subscriptions, and its entry in `peers` and `peer_states`. `remove_peer` returns `PeerRemoveResult` with the removed grant ids and attached subscriptions, and `daemon_transport.rs` detaches those subscriptions. On ordinary close, siblings keep working. On ultimate close failure, the peer is moved to `stale_close_peers` and Hub sacrifices every peer on the dedicated runtime, per [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]. After the move, `peer.rs` is the single owner of all three maps. Oracle: `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`, moved to `peer.rs` with its name and body unchanged, and its self-scan retargeted to `peer.rs`.

`teardown_bounds`: `LOCAL_WEBRTC_PEER_CLOSE_BOUND` bounds `close()`; `LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE` bounds the handler join. A hang, not an `Err`, takes the same fail-closed path as an error: the peer moves to `stale_close_peers`, and `park_runtime_if_idle` or `stop_all` forces driver stop. Both constants and both paths move to `peer.rs` together. Oracles preserved by name: `hanging_data_channel_local_close_still_runs_cleanup_once_within_bound`, `production_on_close_hangs_local_close_and_still_cleans_up`, `hung_send_text_times_out_within_close_bound`, `ready_send_completes_before_queued_on_close`.

`late_message_matrix`: no row changes. Each row states its owner file after the move. I traced the rows through `run_data_channel` (the `ownership_request` and `entity_subscription_change` handling at `src/local_webrtc.rs:1496-1587`), `LocalWebrtcPeerState::cleanup_once` (`:1017-1041`), and the `ControlMessage::LocalWebrtcPeerClosed` handler in `src/daemon_transport.rs:950-1140`.

| Message type | Creates durable ownership | Owner file after move | Owner tag | Rejection after terminal failure | Residual sweep on PeerClosed race |
|---|---|---|---|---|---|
| Second `on_data_channel` | no | `peer.rs` claim, `subscription_channel.rs` reject | peer-scoped one-shot claim | `reject_extra_data_channel` closes the unclaimed channel | none needed; no ownership is created |
| Encrypted Hello | no | `control_channel.rs` | session key derived from the admitted grant | decrypt failure ends the channel with a typed cause | peer cleanup drops the channel task |
| `Attach` | **yes** | `subscription_channel.rs` records via `apply_subscription_change`; `daemon_transport.rs` detaches | `(session_id, subscription_id)` in `peer_state.attached_subscriptions`, plus `attach_owner_grant_ids` keyed by `grant_id` in Hub state | `response_records_attach_ownership` returns false for `OperatorError` and for `attach_failed`, so no ownership row is created | `cleanup_once` snapshots `attached_subscriptions` into `LocalWebrtcPeerClosed`; `detach_local_webrtc_subscriptions` sweeps them, and `attach_owner_grant_ids.retain` drops only rows owned by a removed grant |
| `Detach` | retires ownership | same as `Attach` | same identity | a failed `Detach` leaves the row, so cleanup still sweeps it | idempotent; the sweep tolerates an already-removed row |
| `SubscribeEntities` | **yes** | `control_channel.rs` records via `add_entity_subscription`; `daemon_transport.rs` owns the registry | `subscription_id` on `peer_state.entity_subscription_ids`, and `owner_grant_id` on `state.entity_subscriptions` | the row is added only when the response kind is `EntitySubscribed`, so a failed subscribe creates nothing | `cleanup_once` snapshots `entity_subscription_ids`; the handler removes a snapshot id **only** when the current row is unowned or still owned by a removed grant, and independently removes every row owned by any removed grant |
| `UnsubscribeEntities` | retires ownership | `control_channel.rs` | same identity | removal is applied only on `EntityUnsubscribed` | a late unsubscribe after cleanup is a no-op against an already-removed row |
| `SubscribeEvents` | **yes** | `control_channel.rs` ingress; `src/subscription/package_events.rs` owns the holder | connection-scoped holder keyed by `grant_id` through `event_plane.mailbox(&grant_id)` | admission is exact `(owner, name)`; a rejected contract creates no holder | `state.event_plane.cleanup_connection(grant_id, router)` runs inside the removed-grant loop, so holders retire with the peer. Admitted jobs survive until Core completion by design. |
| `UnsubscribeEvents` | retires ownership | `control_channel.rs` ingress; `daemon_transport.rs:3472` applies it | same connection-scoped holder identity | a late unsubscribe cannot retire another peer's holder, because the holder key includes `grant_id` | `cleanup_connection` is idempotent for an already-retired holder |
| `Spawn` | **yes, and it is intentionally peer-independent** | `daemon_transport.rs` control dispatch; does not move | `session_id`; the session worker is durable and Hub-owned, not peer-owned | a failed spawn creates no session | **not swept by PeerClosed.** A spawned session deliberately survives peer cleanup; only `ShutdownSession` or Hub stop retires it. This is existing policy, restated here so the move does not silently acquire a session sweep. |
| Terminal and entity frames | no | `control_channel.rs` and `delivery.rs` | subscription id on the frame | prior-generation frames are rejected | the channel closes with the peer |
| Host events | no | `control_channel.rs` | negotiated close-event feature per peer | not delivered without negotiation | host-event queue dies with the peer state |

Oracles preserved by name: `replacement_peer_rejects_prior_generation_frames_and_delivers_current_generation`, `entity_subscription_multiplexes_after_ack_and_cleans_up_with_peer`, `peer_admits_only_the_first_data_channel`, `reject_extra_data_channel_closes_the_unclaimed_channel`, `extra_channel_close_marker_requires_lost_claim_and_close_ok`.

`production_path_proof`: The production path is unchanged: peer connection state change or channel close, then `LocalWebrtcHandler::on_connection_state_change`, then `LocalWebrtcPeerState::observe_peer_connection_state`, then `cleanup_once`, then `remove_peer`, then `detach_local_webrtc_subscriptions`, then `park_runtime_if_idle`. After the move the first four steps live in `peer.rs`. The live oracles are the existing `tests/hub_daemon_lifecycle/webrtc_proofs.rs` lanes and the moved in-crate close tests, which drive real peers through `PeerHarness` and assert driver-thread and worker idle. This ticket adds no new live proof; it must show the same lanes still pass and still target the production handler. Terminal record files alone remain insufficient, per [[terminal webrtc failure records do not prove peer runtime teardown]].

`ownership_identity`: `grant_id` identifies the peer. `(session_id, subscription_id, generation)` identifies a terminal route. A delayed `PeerClosed` snapshot cannot delete a row now owned by a live replacement peer, because `remove_peer` snapshots the removed grant ids and the detach path is keyed on them. Both the identity fields and the snapshot logic move to `peer.rs` unchanged.

`sibling_fail_closed_policy`: On successful close, siblings keep working, and the dedicated runtime survives until the peer map empties. On ultimate close failure, Hub sacrifices every peer on the dedicated runtime and sweeps all owners together, per [[webrtc peer cleanup removes every per peer owner together]]. The policy and its blast radius are unchanged and stay proven by the moved `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` test.

## Baseline Suite State

The base is green. This section records how it got there, because revision 1 and revision 2 both planned against a red base.

**History.** Plan Review measured `webrtc_terminal_output_is_byte_exact` failing under full `./test.sh --locked` at the former base `38d140c`, passing in isolation. Its oracle polled for four expected bytes inside a fixed `Duration::from_secs(8)` wall-clock deadline over a live PTY writer, so full-suite concurrency could expire the deadline before the first frame arrived.

**Decision.** Human decision `question_1787999576_734551` ruled that this ticket blocks on the repair rather than working around it. The reason is specific and correct: the failing proof exercises the same WebRTC delivery path this ticket moves, so comparing base and HEAD failure sets cannot exclude a **new load-only regression** introduced by the move. Revision 2's differential protocol is withdrawn and must not be used as final acceptance evidence.

**Repair, now merged.** `ticket_1787999248_674913` is closed. Blocking dependency `dependency_1787999625_716785` is satisfied. The repair reached `main` in five commits, `60aa4c4` through `ddb2de9`, and changed only `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` plus its plan and report documents. It replaced the ambient deadline with an explicit `wait_for_producer_ready` gate and added typed timeout messages that distinguish "no producer-ready frames" from "incomplete producer-ready marker" from "no adapter frames after release". The byte-exact assertion and the test name are both preserved, which is what the owner ticket required.

**Consequence for this plan.** Acceptance check 7 is an absolute full-suite gate again. There is no pre-existing failure to excuse, so any failure at HEAD is this ticket's to explain.

## Acceptance Checks And Tests

Ownership checks, which prove this is an extraction and not a file split:

1. `src/local_webrtc.rs` and `src/webrtc_terminal_adapter.rs` do not exist. Prove with `git ls-files src | grep -E 'local_webrtc\.rs|webrtc_terminal_adapter\.rs'` returning no output.
2. No forwarding facade exists. Prove that no file in `src/` re-exports a WebRTC symbol from a path outside `src/transport/webrtc/**`, and that `src/lib.rs` sources its `pub use` from `crate::transport::webrtc`.
3. Each WebRTC state machine has exactly one owner file. Add a source guard that asserts, for each of `LocalWebrtcTransport`, `LocalWebrtcPeerState`, `LocalWebrtcFlowControl`, `WebRtcConnectionMux`, `WebRtcTerminalAdapter`, `PendingLocalWebrtcRequest`, and `LocalWebrtcAttachedSubscription`, that its `struct` or `enum` declaration appears in exactly one `src/transport/webrtc/**` file and in the file named by "Affected Surfaces And Files". Include one paired absence assertion per symbol against the other five role files, per [[code moves need paired absence and presence source guards]].
4. Behavior-preservation needles are unchanged. Prove that the exact needle strings asserted at `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:92` and `:429` are byte-identical to base; only the `hub_source` path argument changes. A needle edit fails this check.
5. `src/transport/webrtc/**` contains no admission, route, grant, label, or product-policy symbol. Extend the forbidden-identifier guard pattern from `src/transport/shared.rs` to the WebRTC directory, with the same identifier list minus the WebRTC names it legitimately owns, and one red-on-revert arm.
6. `src/lib.rs` `architecture_summary` no longer carries a `local_webrtc` crate-root row, and the crate-root module list test and the internal-visibility list test both pass without it. The `transport` row still classifies `Internal` and `AlreadyInternal`.

Behavior-preservation checks:

7. **Absolute full-suite gate.** `./test.sh --locked` passes at final HEAD on a quiet host, with zero failures. The base `ddb2de9` is green, so no failure may be attributed to a pre-existing condition. If any test fails, the Implementer must either fix it inside move-only scope or stop and report; the charter rule against blanket "unrelated" excuses applies with no differential fallback. These WebRTC lanes must appear in the run and must pass: `webrtc_proofs.rs`, `webrtc_terminal_adapter.rs`, `webrtc_fixtures.rs`, `subscription_ownership_baseline.rs`, `event_plane_saturation.rs`, `shutdown.rs`, `sessions.rs`, `cli.rs`.
   - 7a. `webrtc_terminal_output_is_byte_exact` must pass under the full suite, not only in isolation. It is the proof that most directly covers the moved delivery path, and it is the reason this ticket was blocked.
   - 7b. Record the full-suite result in the implementation report. A green isolated rerun is not a substitute for a green suite run.
8. Exact proof-name preservation. Capture the full test inventory at base commit `ddb2de9` and at final HEAD with `cargo test --workspace --locked -- --list`, and store both. Compare the multiset of leaf test names, meaning the final path segment of each entry. The two multisets must be identical: no proof name may be removed, renamed, or reduced in count. Record a separate explicit rename map from each old module path to its new module path, and require every entry to keep the same leaf name. Any intentionally removed duplicate must be listed by name with its reason. An unexplained difference fails the check.
9. No protocol, DTO, serde name, encryption, framing, chunking, limit, or scheduling change. Prove with `git diff` over `src/client_api_dto/`, `src/daemon/error.rs`, and every `serde` attribute in the diff, showing zero changes, and with a `git diff` over the moved delivery functions showing pure relocation.
10. The Core pin is unchanged. Prove with `git diff` over `Cargo.toml` and `Cargo.lock` showing no `botster-core` revision change and no `webrtc` version change.

Guard checks:

11. Both guard families are enumerated before and after the move with `grep -rn "include_str!" src` and `grep -rn "hub_source(" tests`, and both enumerations are recorded in the implementation report. Each of the nine base expressions in the "Context Loaded" table is accounted for by row, with its post-move target path or an explicit statement that it is unchanged. A count alone does not satisfy this check.
12. Every new `src/transport/webrtc/**` file, including `src/transport/webrtc.rs`, appears in the `src/lib.rs` forbidden-construct guard list.
13. Each added guard-list entry has its own red-on-revert ablation, per [[fixed source guard lists need one ablation per added file]]. One representative arm is not accepted.
14. Every region-bounded guard is re-derived after the move and carries a positive anchor assertion, so an empty or subject-free region fails, per [[region bounded source guards need a required symbol anchor]]. This covers the `on_data_channel` region split in `subscription_ownership_baseline.rs` and the retargeted self-scan for the ultimate-close-failure oracles.
15. The scanner final-state invariant at `src/lib.rs:1126` covers every new file: each guard-list file leaves `cfg(test)` skip mode closed at end of file. Base state is `skip_open_at_eof = false` for both split sources; the post-move measurement must also be `false` for all seven new files.
16. Each new file that holds a moved `#[cfg(test)]` block has its own seeded-tail red arm: a forbidden production construct placed after that file's final `#[cfg(test)]` block makes the guard fail. One shared arm is not accepted.
17. Every `cargo test --exact` filter used in any ablation arm uses the full module path and shows a one-test baseline before the ablation loop, per [[exact Rust test ablations require a one test baseline]] and vault gap 6 below.

Commit shape:

18. The move lands as **one compiling move-only commit**, as the ticket acceptance requires. That single commit carries the file moves and every change made necessary by the move itself: import paths, module declarations, `src/lib.rs` wiring, and guard retargeting. It changes no behavior and must compile on its own. Review it with `git diff --color-moved=dimmed-zebra`. Guard additions that are new assertions rather than retargeting, such as the acceptance check 3 and check 5 guards, land in a separate follow-up commit so the move commit stays purely a move. No commit mixes a move with a semantic change.

Gate commands, run from the worktree with `RUSTUP_TOOLCHAIN=1.97.0` and with `CARGO_TARGET_DIR` unset:

19. `rustc --version` recorded from the same shell, showing `1.97.0`.
20. `cargo fmt --all -- --check`.
21. `cargo clippy --workspace --all-targets --locked -- -D warnings`, rerun in full after each repair, per [[strict clippy can hide later crate diagnostics behind the first compile failure]].
22. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` before the suite.
23. `./test.sh --locked` on a quiet host. Any failure must be matched to its own named marker before anyone calls it unrelated.

Downstream proof required by the charter:

24. This ticket changes no public export and no UI contract. `botster_hub::LocalWebrtcError` and `botster_hub::LocalWebrtcTransport` keep their crate-root paths, so the generic-client cost is zero. Record the zero-cost claim with the evidence for checks 8 and 9 rather than rebuilding `botster-tui` and `botster-web`. If any public export or DTO does change, the ticket has left move-only scope, and the Implementer must stop and re-plan.

## Vault Gaps Worth Capturing

1. A move that splits one file into a role family needs a shared `cfg(test)` harness owner. No current note says where that harness goes. The rule used here is: a harness shared by three or more role files belongs to the module root, not to a role file. Capture the rule and the reason, which is that a role file holding the shared harness becomes the de facto owner of every sibling's tests.
2. Hub's WebRTC "subscription channel" role currently owns rejection, not acceptance, because the dedicated-DataChannel topology has not landed. A file named for the target topology while it implements the current one is a review trap. Capture the convention that a decomposition file may be named for its target role only when its current production content is stated in the plan and the report.
3. Exact multi-line indented needles in `hub_source` guards are move-fragile in a different way from symbol needles: reindentation breaks them silently at the level of evidence quality, not compilation. Capture that a move ticket must forbid needle edits and allow only path retargeting.
4. The `local_webrtc` name survives in constants, environment variables, terminal record file names, and operation strings after the module is gone. Capture that a module retirement does not license renaming its wire-visible or file-visible identifiers, because those are protocol and evidence surfaces.
5. A region-bounded self-scan loses coverage when a function between its two anchor symbols moves out of the file. The region stays green and blind, and retargeting the end anchor is not the fix. [[region bounded source guards need a required symbol anchor]] requires a positive anchor but does not state the move-driven loss case. Capture the loss case.
6. `cargo test --exact` requires the full module path. A bare leaf name filters out every test and still reports `ok`, which turns a guard ablation arm falsely green. This ticket moves 54 tests into new module paths, so every existing exact filter is at risk. No vault note records this. Capture it.
7. A dependency version premise must come from the manifest in the worktree, not from a vault note that records a past state. Revision 1 of this plan asserted `webrtc 0.20` from charter prose while `Cargo.toml` said `0.21.0-beta.2`. Capture the rule that a plan reads the pin, and that a note naming a version is evidence of history, not of the current tree.
8. A pre-existing test failure inside the blast radius of a refactor is not the same class as a pre-existing failure outside it. Human decision `question_1787999576_734551` ruled that a base-versus-HEAD failure comparison plus isolated HEAD success cannot exclude a **new load-only regression** when the failing proof exercises the same path the ticket moves. Revision 2 of this plan proposed exactly that differential protocol and was overruled. Capture the distinction and the rule: when the red proof covers the moved path, block on the repair and require an absolute suite gate; a differential oracle is acceptable only when the failure lies outside the change's reach.
