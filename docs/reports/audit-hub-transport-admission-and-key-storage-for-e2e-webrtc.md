---
description: Audit of botster-hub transport admission and key storage gaps before E2E browser WebRTC implementation.
---

# Audit Hub Transport Admission and Key Storage for E2E WebRTC

## Scope And Assumptions

This report audits the current `botster-hub` repository for transport surfaces, browser admission policy gaps, and key storage readiness before implementing production browser WebRTC. It does not implement WebRTC, edit the web repo, or change the old monolith.

Assumptions applied:

- "E2E WebRTC" means local and remote browser clients use the same encrypted Botster stream after admission; only the admission ceremony differs.
- The hub owns concrete client transport adapters, local signaling, package admission, pairing policy, key storage policy, and provider/cloud signaling hooks.
- `botster-core` owns reusable crypto and identity mechanisms. Hub/CLI/provider layers own concrete persistence, prompts, registration, and runtime policy.
- WebRTC transport ordering is owned by the DataChannel. Botster sequence/transcript fields are for integrity, duplicate/drop detection, reconnect diagnostics, and crypto transcript binding only.

## Current Transport Matrix

| Surface | Entry Points | Classification | Current Evidence | Production Browser Verdict |
| --- | --- | --- | --- | --- |
| Local daemon Unix socket | `botster-hub-client::DaemonConnection`, `request`, `stream_attach`; server in `src/daemon_transport.rs` | Production same-device control plane; terminal attach ingress into shared client data plane | `serve_daemon` binds `TransportBindings.local_socket`; `handle_connection` performs daemon hello, frames `DaemonRequest`, and submits to the hub owner thread | Production candidate for same-device operator clients, not browser WebRTC |
| CLI commands | `src/main.rs` commands using `daemon_transport_request` | Operator control plane over daemon socket | Package, session, status, app, dogfood, and dev-stack commands frame public `DaemonRequest` values | Not browser transport |
| TUI / same-device attach | `stream_attach`, `Attach`, `SendInput`, `Resize`, `Drain` | Same-device client over hub daemon protocol; terminal data path enters core `SessionIo` / `ClientWorker` abstractions | `handle_runtime_control_request` constructs `HubClientApi::local_operator`, then calls `HubClientApi::handle_request`; attach/input/resize flow into `HubRuntime` | Correct data-plane shape to reuse behind a future transport adapter |
| Package app launch | `DaemonRequest::StartPackageEntrypoint`, `ListApps`, `ResolveAppLaunch`; `EntrypointSupervisor` | Hub-owned local/dev process launch contract | `StartPackageEntrypoint` builds supervised environment and starts the package; `ListApps` projects runnable entrypoints plus supervisor snapshots | Useful for local installed app bootstrap, not a browser data plane |
| `botster-web` dogfood bridge | `botster-hub dogfood`, `dev-stack bootstrap`, printed `bridge=` / `web=` URLs | Dev harness / package app surface over localhost HTTP | `start_botster_web_dogfood` enables `botster-web`, injects `BOTSTER_HUB_SOCKET`, `BOTSTER_HUB_DATA_DIR`, and `BOTSTER_WEB_DOGFOOD_BRIDGE_PORT`, then requires structured `local_url` and verified HTML shell. When no port is supplied, dogfood selects an ephemeral `127.0.0.1:0` port with `TcpListener`; that is bridge port probing, not `TransportBindings.tcp`. | Must not be treated as production browser WebRTC |
| Plugin HTTP capability runtime | `HubCapabilityRuntime` HTTP runtime in `src/capabilities.rs` | Plugin capability surface after package admission | Hub policy grants localhost/http(s) capability operations to plugins | Not client browser transport |
| Plugin WebSocket capability runtime | `InMemoryWebSocketCapabilityRuntime` in `src/capabilities.rs` | Plugin network capability surface | Runtime accepts plugin network operations through core capability contracts | Not client browser WebRTC |
| TCP binding config | `TransportBindings.tcp` in `src/config.rs` | Config shape only | Config validates host/port, but repo inspection found no daemon listener using TCP bindings | Scaffold only; not a production transport today |
| MCP tools | `src/mcp.rs` over daemon request kinds | Coordination/control plane | MCP exposes status, identity, messaging, and plugin tool operations through hub surfaces | Not terminal/browser data plane |

Production path proof:

- Daemon socket path: `botster-hub-client` -> `src/daemon_transport.rs` -> `HubClientApi` -> `HubRuntime`.
- Package app path: `StartPackageEntrypoint` / `ListApps` -> `EntrypointSupervisor` -> structured `launch_target.local_url`.
- Terminal data path: `Attach`, `SendInput`, `Resize`, and `Drain` enter `HubClientApi::handle_request` and then `HubRuntime`; the hub socket adapter is framing/control, not a private session-worker protocol.

## Key Storage Verdict

Key storage sufficiency is a two-part verdict.

Core mechanism present: yes.

- `Cargo.lock` pins `botster-core` and `botster-core-daemon` to `42538009bc6f6291872c5657bedbe7370f504f8d`.
- The local Cargo git checkout for that exact revision confirms:
  - `crates/botster-core/src/identity/keyring.rs` defines `CredentialRecord` and `trait CredentialStore`.
  - `crates/botster-core/src/identity/crypto.rs` defines `AesGcmKey` and AES-GCM envelope helpers.
  - `crates/botster-core/src/identity/device.rs` defines `device_fingerprint()` and `verify_device_fingerprint()`.
  - `crates/botster-core/src/lib.rs` re-exports those identity, crypto, and keyring APIs.
- This matches [[botster core owns reusable crypto and identity mechanisms]]: core owns mechanism and shape, not concrete keychain policy.

Hub persistence/policy wired: no.

- `src/config.rs` stores `HostIdentity { id, display_name, fingerprint }` as metadata only.
- `src/persistence.rs` persists that host metadata, package registry state, capability grants, package admission history, local runtime settings, and audit records in `hub-state.json`.
- Package configuration secrets are write-only/redacted markers in package state and daemon DTOs. The repo has tests ensuring raw secret values do not appear in persisted state or client output, but that is not OS keychain-backed secret storage.
- Repo inspection found no hub wiring for `CredentialStore`, OS keychain/keyring, long-lived signing-key persistence, trusted browser device identities, local bootstrap grants with expiry/revocation, or file encryption key storage.

Follow-up owner list:

- Hub/CLI/provider: implement a concrete `CredentialStore` provider using OS keychain with file fallback for test/dev modes.
- Hub: persist or reference long-lived hub private identity through a non-exportable signing boundary instead of metadata-only `HostIdentity`.
- Hub: persist trusted browser public identities and fingerprints, with revocation and audit records.
- Hub: store short-lived local bootstrap grants keyed by hub identity, package/app instance, origin, peer/session id, expiry, and revocation state.
- Hub/provider: define file encryption key storage policy over existing core `AesGcmKey` / envelope primitives.
- Do not file "create core credential primitives" unless a future lockfile revision removes the APIs listed above.

## Local And Remote Browser Admission Policy

Local installed `botster-web`:

- The local package app may request a short-lived localhost bootstrap grant.
- The grant must be scoped to one hub identity, one package/app instance, one expected localhost origin, one peer/session route, and a tight expiry.
- Installed package status and loopback origin are not durable trust by themselves.
- After grant redemption, the browser must use the same encrypted Botster stream protocol as remote browsers.

Remote web:

- Remote browsers require QR/device approval/account-mediated approval before receiving equivalent trusted browser identity.
- Pairing UI ownership should remain in the botster-web Share modal per [[botster web pairing ui lives only in the share modal]].
- Cloud/account or relay providers may mediate discovery and approval, but they should not see private key material or plaintext session contents.

Shared post-admission rule:

- Local and remote admission both unlock or create a trusted browser identity and session keys.
- After admission, both paths converge on the same encrypted Botster stream, same request/response semantics, same terminal data path, and same reconnect behavior.

## Hub Transport API Needed For E2E WebRTC

The follow-up implementation ticket should add hub-side local signaling and admission without replacing existing runtime boundaries:

- Local signaling endpoint: narrow localhost route or package-bridge adapter scoped to a bootstrap grant and peer/session id.
- Offer/answer/ICE lifecycle: grant-bound state with timeout, explicit cancel/cleanup, and diagnostics for no grant, expired grant, signaling failure, ICE failure, and peer teardown.
- DataChannel attach: exactly one ordered/reliable DataChannel per Botster client stream, attached to the transport-neutral `HubClientApi` / core client stream path.
- DataChannel options: use `ordered: true`; do not set `maxRetransmits` or `maxPacketLifeTime` in the first slice.
- Reconnect: probe peer and DataChannel liveness, rebuild when needed, and replay browser route/entity/surface pulls in line with [[botster browser pull requests must retry after webrtc reconnect]].
- Request gating: browser consumers should enter through connection-owned operation gates in line with [[botster webrtc request consumers should use operation gates not connection checks]].
- Settings exposure: package/web settings may expose safe local bootstrap status and pairing actions, but not raw socket paths, data dirs, grants, private identity, or key material.

## Transport Ordering

Ordering is a fixed design constraint for future WebRTC work:

- WebRTC DataChannel ordering is authoritative.
- Botster sequence/transcript fields may detect duplicate, replayed, dropped, truncated, or reordered frames.
- Sequence/transcript fields may contribute to reconnect high-water marks and E2E crypto transcript validation.
- On anomaly, the client or hub should detect and abort or request replay after reconnect.
- The implementation must not add an application-layer reorder buffer, resequencing queue, holdback timer, or normal-delivery buffering mechanism over the DataChannel.

## Side-Band Metadata / OSC-Spam Lane

The browser WebRTC audit also needs a side-band metadata lane because terminal metadata can churn independently from user-visible terminal bytes. Title changes, working-directory updates, prompt marks, bells, notifications, and mode changes must not be allowed to starve attach, input, resize, shutdown, or terminal output delivery.

Old-stack reference design:

- `session/mod.rs` carried latest-win flags and anti-spam handling for high-churn terminal metadata.
- `worker/session_io_runtime.rs` used `SessionIoCoalescer` thresholds of 32 KiB output, 16 frames, and a 4 ms window.
- `worker/client.rs` used `DeliveryKind` lanes and bounded egress to keep non-critical metadata from consuming critical delivery capacity.
- `protocol.rs` had separate frame-type lanes for PTY bytes, resize/control, title/cwd/prompt/bell/notification/mode, snapshot, and lifecycle frames.

New-stack inventory:

- Present: the pinned `botster-core` revision already carries the taxonomy and coalescing contract. `contract/session_protocol.rs` defines frame types for PTY input/output, resize, snapshot, process exit, ping/pong, shutdown, mode flags, title changed, bell, cwd changed, prompt mark, notification, color profile, and spawn. `contract/actor.rs` defines `SessionIoCoalescingPolicy` with default 32 KiB / 16 frames / 4 ms thresholds, `metadata_age_expired`, and `SessionIoOrderedEvent` for prompt, bell, notification, process exit, EOF, desync, and shutdown.
- Partial: metadata frame contracts exist, but repo inspection shows no hub-side browser transport lane classification today. `HubClientObservationKind` only surfaces `SessionActivity`, `Subscription`, `Backpressure`, and `RoutedEnvelope`; plugin capability backpressure exists separately in `src/capabilities.rs`.
- Absent/gap: the old concrete producer wiring from terminal OSC callbacks into classified metadata frames has not been ported into the new session-worker/terminal runtime. Until that is done, PTY output is effectively forwarded through the terminal byte path without the old latest-win metadata lane.
- Gap: `botster-session-worker` uses a bounded `sync_channel` for egress. The future WebRTC transport must make slow-consumer behavior explicit instead of silently losing the difference between safe metadata shaping and unsafe control/terminal-byte loss.

Required invariant for the future single ordered/reliable DataChannel:

- Metadata shaping happens before enqueue into the DataChannel transport, at the hub egress scheduler or equivalent transport adapter boundary.
- Shaped classes include high-churn OSC metadata such as title OSC 0/2, cwd OSC 7, prompt OSC 133, bell, notification OSC 9/777, and terminal mode metadata.
- Shaping means coalesce, latest-win, rate-limit, or drop non-critical metadata before enqueue. It does not mean resequencing normal DataChannel delivery.
- Terminal bytes, input, resize, attach/detach, shutdown, lifecycle, signaling, and encryption/auth failures must not be dropped by the metadata coalescing lane and must preserve their transport ordering.

Follow-up tickets:

- CORE-1 (botster-core: `bin/botster-session-worker.rs` + `botster-terminal-ghostty`): Port/defer-completed old OSC/semantic metadata producer wiring into the new core session-worker/terminal runtime. The metadata contract/taxonomy exists; only the producer wiring from old `ghostty_vt` callbacks into classified frames was intentionally deferred during extraction, not lost behavior. Implementation may differ if cleaner, but the anti-spam/latest-win edge case behavior must be preserved for high-churn OSC metadata: title OSC 0/2, cwd OSC 7, prompt OSC 133, bell, notification OSC 9/777, and mode changes. CORE-1 emits classified metadata frames; it does not define hub egress shaping.
- CORE-2 (botster-core): Define explicit slow-consumer semantics for session-worker/client egress so metadata can be shaped while terminal bytes and critical control frames preserve order and backpressure.
- HUB-1 (botster-hub): Add a hub egress scheduler or transport adapter policy that classifies critical vs side-band metadata frames before WebRTC/DataChannel enqueue, with bounded lanes and coalescing in line with [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[botster hub event lanes coalesce repeatable work before rejecting under pressure]], and [[botster hub event storms must be rejected before queues grow unbounded]].
- WEB/TRANSPORT-1 (botster-web / browser transport): Consume the classified metadata lane through operation gates and reconnect replay without adding app-level reorder buffers over the ordered/reliable DataChannel.

Acceptance check for future implementation work: the transport audit and implementation tickets must classify spammy terminal metadata into a non-critical side-band lane, and must explicitly assert that terminal bytes, input, resize, attach/detach, shutdown, lifecycle, signaling, and auth/encryption failures are protected from metadata coalescing/drop.

## Non-Invasive Guard Added

`docs/client-protocol.md` now states that supervised web app `local_url` / dogfood bridge URLs are local package app/dev harness outputs, not production browser transport or an E2E WebRTC substitute. It also states that production browser transport must be a separate admitted encrypted WebRTC stream over one ordered/reliable DataChannel.

No Rust guard was added because there is no production WebRTC code path to assert yet. The existing bridge regression test `cli_dogfood_launcher_bridge_request_endpoint_uses_same_daemon_state` remains useful for daemon consistency, but it proves only the supervised local bridge harness.

## Ticket-Ready Follow-Up Plan

1. Wire a hub credential provider over existing core `CredentialStore`, with OS keychain and test/dev file fallback behavior.
2. Add hub-owned browser identity state for trusted device public metadata, fingerprint verification, revocation, and audit records.
3. Add local bootstrap grant storage with expiry, one-time redemption, origin/app/peer scoping, and explicit revocation.
4. Add local signaling endpoints or package-bridge signaling adapter for offer/answer/ICE, scoped to the bootstrap grant.
5. Add WebRTC DataChannel transport adapter that feeds the existing `HubClientApi` / core client stream path.
6. Add diagnostics for grant, signaling, ICE, DataChannel, core attach, encryption/auth, and sequence/transcript anomaly failures.
7. Add browser-side operation gates and reconnect replay behavior in the web repo, with pairing UI kept in the Share modal.

## Verification Evidence

Commands and inspections used for this report:

- `project_pipelines_current_context`: loaded ticket, run, approved plan revision, Plan Review approval, findings, answered question, gates, and existing checklist evidence.
- Vault notes: loaded required Implement playbooks and Botster overlay notes, plus governing crypto/WebRTC/pairing/dogfood bridge notes.
- `rg` inventory over `src`, `crates`, `docs`, `tests`, `examples`, and `Cargo.toml` for transport, bridge, identity, credential, secret, bootstrap, admission, WebRTC, and app launch terms.
- `Cargo.lock`: confirmed `botster-core` / `botster-core-daemon` source revision `42538009bc6f6291872c5657bedbe7370f504f8d`.
- Cargo git checkout at `42538009bc6f6291872c5657bedbe7370f504f8d`: inspected `identity/keyring.rs`, `identity/crypto.rs`, `identity/device.rs`, and `lib.rs` re-exports.
- Cargo git checkout at `42538009bc6f6291872c5657bedbe7370f504f8d`: inspected `contract/session_protocol.rs`, `contract/actor.rs`, and `bin/botster-session-worker.rs` for metadata frame taxonomy, coalescing policy, and bounded egress evidence.
- `src/daemon_transport.rs`, `src/client_api.rs`, `src/config.rs`, `src/persistence.rs`, `src/auth.rs`, `src/capabilities.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `docs/client-protocol.md`, and `tests/hub_daemon_lifecycle_test.rs`: inspected concrete entry points and tests listed above.

## Residual Risk

- This report does not verify a real botster-web production bridge because this repo contains only the hub-side package/dogfood harness and tests.
- `TransportBindings.tcp` remains a config shape without a production listener in this repo; future TCP work should either wire it or document it as scaffold-only.
- Browser-side operation gates, reconnect replay, and pairing UI behavior are referenced from vault guidance but not implemented or tested here.
- Checklist creation for the Implement step timed out in the Project Pipelines plugin worker; the same vault/checklist evidence is recorded in this durable report and gate evidence instead.
- Side-band metadata lane producer wiring and hub egress shaping are intentionally follow-up work. This audit names the lane, required invariant, and follow-up owners but does not implement producer callbacks or scheduler behavior.

## Vault Capture Candidates

- Hub browser admission vocabulary: local bootstrap grant vs remote device/account approval, converging on one encrypted stream.
- Hub credential-store wiring owner split over existing core primitives.
- Dogfood HTTP bridge classification as dev harness, not production browser transport.
- Single ordered/reliable DataChannel plus detection-only sequence/transcript rule as a reusable WebRTC planning constraint.
- OSC/side-band metadata spam lane as a required Botster transport-audit dimension: classify, coalesce/latest-win/rate-limit/drop before DataChannel enqueue while protecting terminal bytes and critical control frames.
