# Implement hub local WebRTC signaling and encrypted client transport adapter

## Context

- Ticket: `ticket_1782857282_831231`
- Run: `run_1782860960_549317`
- Assumption: this ticket owns only ephemeral local `botster-web` bootstrap
  grants, daemon-request signaling, and the hub-side DataChannel adapter.
  Durable trusted browser identity/key storage remains out of scope.
- Human answer authorized adding a real Rust WebRTC/DataChannel stack, provided
  the dependency choice and browser-interop boundary stay explicit.

## Implementation

- Added `webrtc = 0.20.0-beta.2` as the smallest credible async Rust stack
  found that can establish browser-compatible ICE/DTLS/SCTP DataChannels without
  hand-rolling the transport. `str0m` was reviewed but is Sans-IO and would have
  required building the network pump in this ticket.
- Added `src/local_webrtc.rs` behind the hub transport boundary. It mints
  short-lived in-memory grants for installed `botster-web/web-client`, validates
  grant id/secret/origin/expiry/single-use constraints, answers SDP offers, and
  owns active peer connections on a persistent Tokio runtime.
- Wired the production path through `StartPackageEntrypoint` for package
  `botster-web`, so the bootstrap is issued while the real installed package
  entrypoint is launched. The HTTP/SSE dogfood bridge remains intact.
- Added `LocalWebrtcSignal` and local WebRTC bootstrap/answer DTOs to
  `botster-hub-client`, generated TypeScript, and CLI response printing. CLI
  output prints grant id and metadata but not the grant secret.
- DataChannel payloads use JSON serialized `botster_core::AesGcmEnvelope`
  values. Plaintext inside the envelope is the existing `DaemonRequest` /
  `DaemonResponse` protocol; invalid unauthenticated DataChannel frames are not
  answered with a plaintext fallback.
- Follow-up review fixes track terminal `Attach` subscriptions owned by each
  WebRTC peer and submit owner-thread cleanup when the DataChannel or peer
  connection closes, so normal browser tab-close detaches subscriptions and
  prunes the peer registry before daemon shutdown.
- Bootstrap random token generation now hard-fails through a typed local WebRTC
  operator error instead of falling back to a low-entropy clock token.

## Verification

- `cargo test -p botster-hub-client`
- `./test.sh installed_botster_web_launch_issues_local_webrtc_grant_and_data_channel_adapter`
- `./test.sh local_webrtc_peer_close_detaches_terminal_subscriptions`
- `cargo clippy --all-targets -- -D warnings`
- `cargo check --workspace`

The WebRTC acceptance drives the installed package launch, receives the
bootstrap from that production request, rejects wrong-origin and wrong-secret
signals, completes offer/answer with a Rust WebRTC peer, proves ordered/reliable
DataChannel settings, sends encrypted
status/list/spawn/attach/resize/input/drain/shutdown requests, rejects grant
reuse, and checks persisted hub state does not contain the runtime grant
id/secret. The dropped-peer regression closes a WebRTC peer without
`ShutdownSession`, then proves later terminal output reaches a socket
subscription but not the closed WebRTC subscription.

## Residual Risk

- The harness proves hub-side signaling and adapter behavior with a Rust WebRTC
  peer. Real browser `RTCPeerConnection` interoperability is still a botster-web
  parity follow-up.
- Backpressure/lane shaping is not expanded in this slice. The adapter forwards
  one request per received DataChannel message through the existing daemon
  owner-thread request path and does not add an unbounded application queue.
- Durable browser identity, trusted browser persistence, and long-lived secret
  storage remain blocked on the dedicated key-storage dependency.
- The ephemeral bootstrap grant secret is still used directly as the local
  stream key for this slice. A future durable-identity/key-storage pass should
  add explicit KDF/domain separation when that protocol becomes long-lived.

## Missing Vault Guidance

No missing vault guidance was found. The material scope decision was the human
answer authorizing a real WebRTC dependency after the workspace proved no
existing WebRTC/DataChannel stack was present.
