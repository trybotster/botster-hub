# Implement report: Bind content-blind WebRTC terminal adapters at admission

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | this run's Hub worktree |
| Ticket | `ticket_1786661008_247079` |
| Run | `run_1786704125_619383` |
| Step | `botster_stack_implement` (`run_step_1786709213_775323`) |
| Approved plan | `docs/plans/bind-content-blind-webrtc-terminal-adapters-at-admission.md` revision 2 |
| Merge policy | direct (no PR) |
| Locked Core SHA | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Rebased onto Hub main | `aafd6c2cde430804f1bb54094c568fc88c15944b` |
| First implement commit | `fc946d198801a6800de451b3dba41e7537ea3440` |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and the approved Plan artifact both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — optional feature, delivery kind, generated TypeScript
- [[botster runtime teardown lenses]] — required; class applies

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[late webrtc messages after disconnect must not recreate clients]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[adding a hub client feature constant is a three site change]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[hub generated protocol changes are a four site release chain]]
- [[test script required for rust tests not cargo test]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — package/plugin workflow implementation is out of scope
- Other repository charters (Core, Web, TUI, Workspaces, Ghostty)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved revision-2 plan; keep Hub charter ownership
- Do not implement Core, Unix rewrite, Drain cold-cut, Web decoder, TUI, or Project Pipelines
- Use `./test.sh` / `BOTSTER_ENV=test` wrappers
- Advertise `webrtc_terminal_adapter` without raising `DaemonCompatibilityRequirement::current()`
- `PROTOCOL_VERSION` stays 7; keep `CONFORMANCE_FIXTURE_REVISION` at 40 with `terminal_subscription_closed`
- Preserve split Hello `terminal_compatibility` and `TerminalSubscriptionClosed` from `aafd6c2`
- Runtime-teardown lenses are implemented, not deferred

## Files changed

| Path | Change |
| --- | --- |
| `src/webrtc_terminal_adapter.rs` | Production one-slot adapter, per-peer mux, Core harness driver |
| `src/lib.rs` | Private `webrtc_terminal_adapter` module |
| `src/daemon_attach_stream.rs` | `BoundAdapterHandle` enum; WebRTC bind helper; grant close helpers |
| `src/daemon_transport.rs` | DataChannel Hello admission; Attach bind when `grant_id` + feature; bound PeerClosed close-only |
| `src/local_webrtc.rs` | Inbound Hello vs Request; HelloAck; sender-loop flush; `DaemonTerminalFrame` chunking; bounded `local_close`; hang inject; inventory/teardown proofs |
| `crates/botster-hub-client/src/lib.rs` | `FEATURE_WEBRTC_TERMINAL_ADAPTER`, `for_webrtc_terminal_adapter()`, `DaemonTerminalFrame`, revision 40 |
| `crates/botster-hub-client/src/typescript.rs` + `generated/daemon-protocol.ts` | Delivery-kind union includes `daemon_terminal_frame` |
| `crates/botster-hub-test-support/src/lib.rs` | Support matrix lists the feature under supported |
| `packages/hub-test-support/*` | Unpublished `0.1.35`; revision 40 fixtures; feature supported not required |
| `tests/hub_daemon_lifecycle_test.rs` | `include!` of the new WebRTC adapter proofs |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Live bind, unbound Drain, peer-loss close-only, explicit Detach, late attach, compatibility |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Exhaustive delivery-kind matches; unbound receive rejects unexpected terminal frames; Hello/terminal helpers |
| `docs/client-protocol.md` | DataChannel Hello, optional feature, `DaemonTerminalFrame`, bound close-only, close bound |
| `docs/plans/bind-content-blind-webrtc-terminal-adapters-at-admission.md` | Approved plan revision 2 |
| `docs/reports/bind-content-blind-webrtc-terminal-adapters-at-admission-implement.md` | This report |
| `README.md` | Conformance revision 40 |

## Ownership boundaries preserved

- Hub owns admission, route records, adapter instance, framing, encryption, and chunked write.
- Core still owns queues, attach phases, inventory, and mechanical detach. This ticket consumed the locked `f4f6bf5` API.
- External DTOs stay in in-repo `botster-hub-client`.
- Terminal bodies stay opaque (`to_bytes()` only).
- Web, TUI, Unix rewrite, Drain cold-cut, and Core implementation were not edited.

## Cross-repo dependencies or separately routed work

- Closed Hub Unix parent `ticket_1786661008_634435` — reused bind sequence and occupancy rules.
- Closed Core parents already locked at `f4f6bf5babe92dfb9241a760c414187f711c2c42`.
- Sibling Web `ticket_1786661008_897067`, TUI `ticket_1786661009_551067`, cold-cut `ticket_1786661010_198387`, and integration `ticket_1786661010_115885` remain separately routed. This run did not edit those repositories.
- npm publish of `@trybotster/hub-test-support@0.1.35` stays a later Hub-owned release step (sites 3–4 of the four-site chain).

## Deviations from plan

- HelloAck uses encrypted `DaemonHelloAck` plaintext framed as `DaemonLocalWebrtcDeliveryKind::DaemonResponse`. That is the smaller existing DTO. The plan asked Implement to pick this or a control `DaemonResponse`. Documented in `docs/client-protocol.md`.
- Capability intersection reuses `negotiated_unix_capability_set` instead of renaming it. Behavior is the same.
- Current-Web downstream proof used the Hub-owned live packaged-web fixture (`write_botster_web_package` + IsolatedHub DataChannel attach without Hello). The plan allowed that path. This run did not edit `botster-web`.
- In-process inventory teardown injects `RTCPeerConnectionState::Failed` through the production handler. The live IsolatedHub proof closes the offer peer.

No accepted scope change requires rewriting the committed plan contract.

## Runtime-teardown lenses

Every lens from [[botster runtime teardown lenses]] is implemented. None was waived.

| Lens | Implementation |
| --- | --- |
| Isolation | One grant owns one peer, its DataChannel loop, and every bound adapter for that grant. Host sessions stay listed. Sibling grants stay live unless the shipped fail-closed dedicated-runtime path runs. |
| Bounds | Adapter `try_write` / `close` / `Drop` do not `block_on` DataChannel I/O. `close_data_channel` wraps `local_close()` in `LOCAL_WEBRTC_PEER_CLOSE_BOUND` with no retry. Timeout or error still calls `cleanup_once`. |
| Late-message matrix | Gone-peer Requests still return `local_webrtc_peer_gone`. Hello never creates a route. Late Hello after cleanup is not registered. Unbound residual still Hub-Detaches. Bound residual closes adapters only. |
| Production-path proof | IsolatedHub package bind → bootstrap → signal → DataChannel Hello → Attach Attaching-only → `DaemonTerminalFrame` chunks → peer close → no Hub Detach. Hang inject proves `cleanup_once` within the bound. |
| Ownership identity | Route key is `(grant_id, client_id=botster-hub-webrtc-{grant_id}, session, subscription, generation)`. Cleanup matches live owner+generation. |
| Sibling fail-closed | Existing dedicated-runtime hang/error tests remain. Successful close does not sacrifice siblings. |

## Tests and downstream proof run

Provenance of the tested binaries:

| Identity | Value |
| --- | --- |
| Hub checkout SHA (pre-commit) | `9d1f858fbfaf87ff2e95cf292690b03e91558695` |
| Locked Core SHA | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `botster-hub` realpath | `target/debug/botster-hub` under this worktree |
| `botster-session-worker` realpath | `target/debug/botster-session-worker` under this worktree |

Commands:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
BOTSTER_ENV=test cargo clippy --workspace --all-targets --locked -- -D warnings
./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal
./test.sh --locked
npm --prefix packages/hub-test-support run check
node packages/hub-test-support/test.mjs
```

Results:

- Format check passed.
- Strict locked clippy passed (`-D warnings`).
- `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal` ran 7 new proofs.
- `./test.sh --locked` passed the workspace suite, including hub-client, hub-test-support asset parity, and existing unbound WebRTC proofs.
- Support matrix lists `webrtc_terminal_adapter` under `supported_features` only.
- Unbound receive paths treat unexpected `daemon_terminal_frame` as failure.

Production entry point: IsolatedHub launches this worktree's `botster-hub`. After package bind, `IssueLocalWebrtcBootstrap`, `LocalWebrtcSignal`, and an encrypted DataChannel Hello that requires `webrtc_terminal_adapter`, Attach binds through `HubRuntime::bind_terminal_adapter`. Later frames leave only as `DaemonTerminalFrame` chunks from the admitted DataChannel sender loop.

## Unverified behavior or residual risk

- Authentic Ghostty decoder proof stays on the Web ticket and the integration ticket.
- This run did not launch the `botster-web` repository's `webrtcDaemonClient.ts` against the changed Hub. The Hub-owned live packaged-web fixture without Hello is the current-Web Drain proof.
- A dedicated bound replacement-owner live WebRTC test was not added. Occupancy and owner+generation matching reuse the Unix/existing WebRTC replacement path. Delayed A PeerClosed still must not close B; Review should treat that as covered by the shared occupancy path plus the in-process stale-snapshot proof, not as a new bound-pair live oracle.
- npm `@trybotster/hub-test-support@0.1.35` is unpublished. Downstream Web pin remains `0.1.32`.
- HelloAck on the DataChannel is not a new delivery kind. Bound clients must parse the Hello reply as `DaemonHelloAck`.

## Missing vault guidance discovered

None blocked implementation. After merge, capture:

- WebRTC protocol admission is DataChannel Hello.
- WebRTC revocation is grant `remove_peer`.
- DataChannel `local_close` uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND` before `cleanup_once`.

Do not capture the proposed north star as ratified from this ticket alone.

## Review-fix pass

Rebase and review findings from `review_1786709196_893623`. This pass sits on Hub main `aafd6c2` (split Hello terminal compatibility and `TerminalSubscriptionClosed`).

### Review findings

| Finding | Fix |
| --- | --- |
| `finding_1786709196_581026` Multiple DataChannels can send the same terminal frame | `LocalWebrtcPeerState::claim_data_channel()` admits the first channel only. Extra channels close within `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. Live proof: `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames`. |
| `finding_1786709196_301296` Adapter wake can be lost before the sender waits | `AdapterWake` stores a permit and rechecks after wait registration. Proof: `wait_observes_a_write_that_happens_after_an_empty_scan`. |
| `finding_1786709196_978605` Live peer-loss check permits zero adapter closes | IsolatedHub proof now requires `bound_adapter_close >= 1`, `cleanup_hub_detach == 0`, and occupancy drop. IsolatedHub inventory is `live_attach_subscriptions`. Core inventory absence stays in `webrtc_hello_bind_echoes_capability_set_and_closes_adapter_on_peer_loss`. |

### Control contract preserved from `aafd6c2`

- `DaemonHello` / `DaemonHelloAck` keep independent `terminal_compatibility`.
- WebRTC Hello that fails `ensure_terminal_compatible` stores `WebrtcTerminalAdmission::Rejected`. The next Attach returns `OperatorError`. The DataChannel stays up.
- HelloAck still advertises `TerminalCompatibility::current()`.
- Unix `TerminalSubscriptionClosed` path is unchanged. Bound WebRTC peer loss is connection death, so that event is not emitted. Unbound `Disconnected` stays recoverable.

### Additional teardown fixes required by the live IsolatedHub oracle

- `send_text_or_peer_terminal` bounds hung `local_send_text` with `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and drains DataChannel events during send.
- `OnClosing` is terminal. IsolatedHub can emit it before `OnClose`.
- The idle sender prefers poll over `wait_for_write` and yields after an empty adapter flush.
- `RTCPeerConnectionState::Disconnected` is terminal only when the peer mux has bound adapter routes. IsolatedHub offer-peer close often stays `Disconnected` instead of `Closed`/`Failed`. Unbound recoverable disconnect is unchanged.

### Review-fix files

| Path | Change |
| --- | --- |
| `src/webrtc_terminal_adapter.rs` | Permit wake; `has_bound_routes()` |
| `src/local_webrtc.rs` | One-channel claim; bounded send; `OnClosing`; bound `Disconnected`; split Hello admission |
| `src/daemon_transport.rs` | `WebrtcTerminalAdmission` Admitted/Rejected; Attach reject on mismatch |
| `src/daemon_attach_stream.rs` | `close_from_host`; bind uses Hello terminal requirement |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Two-channel proof; strict live peer-loss oracles |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Extra DataChannel helper; parks terminal frames during request |
| `packages/hub-test-support/metadata.json` | Regenerated hashes after matrix/feature merge |

### Review-fix tests

```sh
./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal -- --test-threads=1
BOTSTER_ENV=test cargo test --locked --lib hung_send_text -- --test-threads=1
BOTSTER_ENV=test cargo test --locked --lib webrtc_hello_bind -- --test-threads=1
BOTSTER_ENV=test cargo test --locked --lib peer_admits_only -- --test-threads=1
BOTSTER_ENV=test cargo test --locked --lib wait_observes -- --test-threads=1
```

Results:

- `webrtc_terminal` 8 passed, including `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`.
- Hung-send, one-channel claim, wake-permit, and in-process bound peer-loss unit tests passed.

Unverified in this pass: full `./test.sh --locked` workspace rerun after the rebase. The first implement pass already ran that wrapper on the pre-rebase commit. This pass reran the review-finding oracles and IsolatedHub live peer-loss.
