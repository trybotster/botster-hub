# Plan: upgrade WebRTC for post-handshake DataChannel creation

Ticket: `ticket_1787654915_646236`
Run: `run_1787654940_337274`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Plan base commit: `f66d459` (clean tracked worktree)

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Repository resolved from the ticket target through `list_spawn_targets`, not from the process
  working directory.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role and surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Class overlay (runtime-teardown class applies):

- [[botster runtime teardown lenses]]

Atomic notes:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[rejected channel isolation needs a surviving channel positive control]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[express scope limits as invariants not closed enumerations]]

[[project-pipelines-playbook]] is not loaded. This ticket changes no Project Pipelines package
or plugin path.

## Context loaded

Repository sources read at base commit `f66d459`:

- `Cargo.toml` — the `webrtc = "0.20.0-beta.2"` requirement and workspace members.
- `Cargo.lock` — the resolved `webrtc 0.20.0-rc.1` and `rtc* 0.20.0-rc.1` entries.
- `src/local_webrtc.rs` (7413 lines) — production peer, handler, and the in-file two-peer test harness.
- `src/local_webrtc_smoke.rs` (488 lines) — the local smoke peer.
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` — the lifecycle-suite WebRTC fixtures.
- `test.sh` — the repository test wrapper (`BOTSTER_ENV=test cargo test --workspace`).

Dependency sources read (unpacked to a scratch directory, not vendored into the repository):

- `webrtc 0.20.0-rc.1` and `webrtc 0.21.0-beta.2` crate sources.
- `rtc 0.20.0-rc.1` and `rtc 0.21.0-beta.2` crate sources and changelogs.

Registry facts confirmed from the crates.io index:

- `webrtc 0.21.0-beta.2` requires `rtc ^0.21.0-beta.2`.
- The newest published `rtc` version is `0.21.0-beta.2`. The resolved `rtc` release is therefore
  exactly `0.21.0-beta.2`, with the `rtc-*` member crates at the same version.

## The enabling upstream change

`PeerConnection::create_data_channel` in `webrtc 0.21.0-beta.2` adds one call that
`0.20.0-rc.1` does not have:

```rust
self.inner.wake_writes().await;
```

In `0.20.0-rc.1` a DataChannel created after the SCTP association is established registers the
channel but does not wake the driver write loop, so the DCEP OPEN is not flushed until unrelated
traffic wakes the driver. `0.21.0-beta.2` wakes writes at creation. This is the exact dependency
capability the ticket needs and the exact behavior the new regression test must prove.

## Scope

1. Raise the `webrtc` requirement in the root `Cargo.toml` from `0.20.0-beta.2` to `0.21.0-beta.2`.
2. Update `Cargo.lock` so `webrtc`, `rtc`, and every `rtc-*` member resolve to `0.21.0-beta.2`.
3. Adapt the runtime-bound `block_on`, `sleep`, and `timeout` call sites to the `0.21` API.
4. Add explicit fail-safe arms to every `match` that `0.21` makes non-exhaustive.
5. Add one two-peer regression test that creates a reliable ordered DataChannel after both peers
   reach `Connected`, and proves the late channel opens remotely and delivers bytes.
6. Record exact dependency versions and migration evidence in the implement report.

### Non-scope

- Subscription reservation, route labels, generations, or channel routing. Those belong to
  `ticket_1787600674_500120` (Hub: isolate control and terminal subscriptions) and
  `ticket_1787600682_233928` (Hub: entity and package-event DataChannels).
- Per-subscription SDP renegotiation and any pre-created channel pool.
- Extraction of any responsibility from `local_webrtc.rs`. The migration touches call syntax and
  match arms only.
- Relaxing the current single-claim policy in `LocalWebrtcHandler::on_data_channel`. A second
  DataChannel from a browser stays rejected in this ticket.
- Any change to signaling, ICE, AES-GCM application encryption, chunking, close bounds, reconnect,
  or peer lifecycle semantics.

## Migration surface, derived from the crate diff

### 1. Free runtime functions become runtime-bound

`0.20.0-rc.1` exported `block_on`, `sleep`, and `timeout` as free functions from
`webrtc::runtime`, backed by the compile-time-selected runtime feature. `0.21.0-beta.2` removes
that selection:

| `0.20` | `0.21` |
|--------|--------|
| `block_on(fut) -> F::Output` | `Runtime::block_on(&self, Pin<Box<dyn Future<Output = ()> + '_>>)` — returns `()` to stay object-safe |
| `sleep(dur).await` | `Runtime::sleep(&self, dur).await` |
| `timeout(dur, fut) -> Result<T, ()>` | `timeout(&dyn Runtime, dur, fut) -> Result<T, Elapsed>` |

`channel`, `Sender`, `Receiver`, and `default_runtime` remain importable from `webrtc::runtime`.
`channel` and the channel types move to `webrtc::runtime::primitives` and are re-exported, and
their `send` / `try_send` / `recv` / `try_recv` signatures are unchanged, so those call sites need
no edit.

`Runtime::spawn` now returns `Box<dyn JoinHandle>` instead of a concrete `JoinHandle`. Hub discards
every returned handle. The tokio backend still detaches on drop in `0.21`, so no spawned poller is
silently cancelled. This must be asserted, not assumed — see acceptance check A6.

#### `block_on` migration decision

`0.20`'s free `block_on` built a fresh multi-thread tokio runtime and returned the future's output.
`0.21`'s `TokioRuntime::block_on` builds the same fresh multi-thread tokio runtime but discards the
output.

Decision: at the three sites that need a return value, build the tokio runtime explicitly
(`tokio::runtime::Builder::new_multi_thread().enable_all().build()`) and call `block_on` on it.
This is byte-for-byte the behavior `0.20` provided, it keeps the return value without a contrived
channel or `Arc<Mutex<_>>` hand-off, and it matches the pattern the surrounding
`src/local_webrtc.rs` tests already use. Rejected alternative: `Runtime::block_on` plus a one-shot
channel to move the value out, which adds a synchronization dance for no behavior gain.

### 2. Non-exhaustive enums

`rtc 0.21.0-beta.2` marks its public state and event enums `#[non_exhaustive]`, including
`RTCPeerConnectionState`, `RTCIceGatheringState`, `RTCDataChannelState`, and `RTCSignalingState`.
`webrtc 0.21.0-beta.2` marks `DataChannelEvent` `#[non_exhaustive]`.

Every existing `DataChannelEvent` match in this repository already carries a `_` arm, so those
compile unchanged. The known break is the exhaustive match in
`src/local_webrtc.rs:1109`, `local_webrtc_peer_connection_state`, which maps every
`RTCPeerConnectionState` variant to a log string.

Fail-safe rule for the added arms, stated as an invariant rather than a fixed edit count:

- A new unknown **state** maps to a neutral, non-terminal label (`"unknown"`). It must not be
  mapped to `Failed`, `Closed`, or any terminal cause, because inventing a terminal cause from an
  unrecognized variant would tear down a healthy peer.
- A new unknown **event** is ignored (`Ok(())`), which is what the existing `_` arms already do.
  It must not be treated as a close or an error.

`cargo check --workspace --all-targets` enumerates the complete set. Implement must add an arm at
every site the compiler reports and must not add a blanket `_` arm that would hide a future
meaningful variant in the two matches that derive a terminal cause
(`src/local_webrtc.rs:1004` already returns `None` for unmatched states, which is already fail-safe).

### 3. Bind-address behavior change is inert here

`webrtc 0.21` re-resolves configured bind addresses on every bind and expands a wildcard
(`0.0.0.0` / `[::]`) into one socket per interface. Every Hub call site passes the loopback
literal `127.0.0.1:0`, so this change does not affect Hub candidate gathering or file-descriptor
count. `PeerConnectionBuilder::build` now requires `A: Send + 'static`; the existing arguments are
owned `String` values and `&'static str` literals, which already satisfy it.

### 4. Crypto provider features

`rtc 0.21` introduces `crypto-ring` (default) and `crypto-aws-lc-rs`, and they are additive. Hub
takes the `webrtc` default features, so the resolved provider stays `ring` and the DTLS and SRTP
wire behavior is unchanged.

## Affected surfaces and files

| File | Change |
|------|--------|
| `Cargo.toml` | `webrtc = "0.21.0-beta.2"` |
| `Cargo.lock` | `webrtc`, `rtc`, and every `rtc-*` member to `0.21.0-beta.2`, plus the new `rtc-crypto` and transitive entries |
| `src/local_webrtc.rs` | `timeout` call sites gain the runtime argument; the `RTCPeerConnectionState` match at line 1109 gains a fail-safe arm; the new regression test is added to the existing in-file two-peer harness |
| `src/local_webrtc_smoke.rs` | import list; one `block_on`, one `sleep`, and the `timeout` call sites |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | import list; two `block_on` sites and the `timeout` call sites |
| `docs/reports/upgrade-webrtc-for-post-handshake-datachannel-creation-implement.md` | migration evidence and exact versions |

No other file is expected to change. If `cargo check --workspace --all-targets` reports a break
outside this set, Implement records it in the report rather than widening scope silently.

## Repository ownership boundaries and cross-repository dependencies

- This is a `botster-hub` dependency and transport-mechanics change. It stays inside the Hub
  trusted host kernel and adds no product policy.
- Core ownership is untouched. Hub remains content-blind to terminal payloads.
- `botster-hub-client`, `botster-ui-contract`, and `botster-hub-test-support` publish no WebRTC
  types and need no version bump. `webrtc` is a root-package dependency only.
- No published protocol revision, DTO, or capability token changes. No `hub-test-support` or
  `ui-contract` cutover applies.
- No cross-repository prerequisite is registered. `botster-web` and `botster-tui` consume no
  `webrtc` Rust API. The Web consumer tickets depend on the *next* Hub ticket
  (`ticket_1787600674_500120`), not on this one.
- This ticket is a prerequisite for `ticket_1787600674_500120` and `ticket_1787600682_233928`.
  Those two tickets own the browser-created subscription channel contract that this upgrade makes
  possible.

## Runtime-teardown class answers

`teardown_class_applies`: **yes**. The ticket changes the WebRTC peer library under
`local_webrtc.rs`, touches the bounded close paths through the `timeout` migration, and changes
when a post-handshake `on_data_channel` event can arrive.

`teardown_isolation`: The ownership set is unchanged by this ticket. One failed peer still tears
down its own `LocalWebrtcPeerState`, its mux routes, and its dedicated runtime, and healthy sibling
peers are unaffected on a successful close. The upgrade adds no new owner and no new durable row.
The migration must not alter which owners die together; Implement changes call syntax and match
arms only.

`teardown_bounds`: `LOCAL_WEBRTC_PEER_CLOSE_BOUND` currently bounds `data_channel.local_close()`
in `reject_extra_data_channel` (`src/local_webrtc.rs:156`), the runtime close path
(`src/local_webrtc.rs:437-441`), and the peer close path (`src/local_webrtc.rs:1692`). Each of
those uses `tokio::time::timeout`, not `webrtc::runtime::timeout`, so the migration does not touch
them — and Implement must not convert them opportunistically. The risk is a rewrite that drops a
bound while editing neighboring `timeout` calls. The named hard stop that ends driver loops is
unchanged: the peer-close bound followed by cleanup regardless of the library close result, per
[[WebRTC DataChannel local close uses the peer close bound before cleanup]] and
[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]].

`late_message_matrix`: This ticket creates no new ownership-creating message type. The one arrival
class the upgrade newly makes reachable is a post-handshake DataChannel open.

| Message / event | Owner tag | Rejection after terminal failure | Residual sweep |
|-----------------|-----------|----------------------------------|----------------|
| `on_data_channel` (first channel) | `peer_state.claim_data_channel()` under `grant_id` | peer terminal cause published; poller exits | existing peer cleanup removes every per-peer owner |
| `on_data_channel` (post-handshake / extra) — newly reachable | claim already taken, so `claimed == false` | `reject_extra_data_channel` closes it under the peer close bound and creates no route | no residual: no adapter is bound, no route inserted |
| Encrypted `Hello` / `Request` on the claimed channel | `grant_id` + stream key | unchanged | unchanged |
| `PeerClosed` | `grant_id` | unchanged | unchanged |

No matrix row gains a new durable owner in this ticket. The reservation-tagged rows arrive with
`ticket_1787600674_500120`.

`production_path_proof`: The production path is
`browser creates DataChannel after Connected → driver flushes DCEP OPEN (the 0.21 wake_writes fix)
→ LocalWebrtcHandler::on_data_channel → claim_data_channel() returns false → reject_extra_data_channel
→ bounded local_close → peer keeps serving its first channel`.

This ticket is **intentionally a dependency-capability upgrade with a preserved fail-closed
production path**. The Hub production answerer accepts exactly one DataChannel today and this
ticket does not change that. Two proofs follow from that, and Implement must produce both:

1. *Positive capability proof, library level*: a two-peer regression test with two test-owned
   peers, proving a channel created after both peers reach `Connected` opens on the remote side and
   delivers bytes. This is the honest oracle for what the upgrade actually buys.
2. *Preserved fail-closed proof, production level*: the existing rejection tests still pass through
   `LocalWebrtcHandler::on_data_channel`, including the surviving-channel positive control required
   by [[rejected channel isolation needs a surviving channel positive control]].

The positive production path — a browser-created subscription channel that Hub *admits* — is the
deliverable of `ticket_1787600674_500120`, and this plan names it rather than claiming it.

`ownership_identity`: Unchanged. `grant_id` plus the peer state remains the owner identity for
every per-peer row. No reused-id policy changes, because this ticket inserts no row.

`sibling_fail_closed_policy`: Unchanged. On a successful close, sibling peers keep working. On an
ultimate local close failure, the documented bounded sibling sacrifice on the dedicated runtime
stands, per [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]].
The regression suite for that behavior must stay green and must not be weakened to accommodate the
new dependency.

## Assumptions and unknowns

Assumptions, stated explicitly:

- A1. `webrtc 0.21.0-beta.2` resolves `rtc` and every `rtc-*` member to `0.21.0-beta.2`, because no
  `0.21.0-rc` is published at plan time. If Cargo resolves a newer prerelease at Implement time,
  Implement records the exact resolved versions and the plan claim is corrected, not waived.
- A2. `wake_writes()` in `create_data_channel` is the change that makes post-handshake creation
  work. This is read from the crate diff and is falsifiable: acceptance check A5 fails on `0.20`.
- A3. The tokio backend's detach-on-drop for spawned tasks is preserved. Asserted by A6.
- A4. No `botster-core` change is required. Core exposes no WebRTC types to Hub.

Unknowns for Implement to resolve, each with a named resolution:

- U1. The complete set of newly non-exhaustive match sites. Resolution:
  `cargo check --workspace --all-targets` enumerates them; apply the fail-safe rule above at each.
- U2. Whether `0.21`'s `timeout`, which is derived from `Runtime::sleep` and `futures::select`
  rather than tokio's timer wheel, changes any test's timing margin. Resolution: run the focused
  WebRTC lifecycle tests and the full wrapper; if a margin is tight, widen the *test* bound and say
  so in the report — do not change a production bound.
- U3. Whether the `rtc-crypto` split pulls a new transitive crate that conflicts with the existing
  `rustls` / `ring` graph used by `ureq` and the installer. Resolution: `cargo check --workspace
  --all-targets` and inspection of the `Cargo.lock` diff.
- U4. Whether the smoke path's `block_on` replacement can be reached from inside an existing tokio
  runtime, which would panic. Resolution: `0.20` had the identical nested-runtime hazard, so this is
  a preserved property, not a new one; Implement confirms the call site is reached from a plain
  thread.

## Risks

- R1. **Dropping a close bound during the `timeout` migration.** The repository mixes
  `tokio::time::timeout` (bounded teardown) with `webrtc::runtime::timeout` (test waits). Only the
  latter changes. Mitigation: the migration edits only imports and `webrtc::runtime::timeout` call
  sites; the close-hang fail-closed test stays green.
- R2. **A blanket `_` arm hiding a future meaningful state.** Mitigation: the fail-safe rule above
  distinguishes neutral-label arms from terminal-cause arms, and no terminal cause may be derived
  from an unrecognized variant.
- R3. **Prerelease dependency churn.** `0.21.0-beta.2` is a beta. Mitigation: `Cargo.lock` is
  committed and the exact resolved versions are recorded in the implement report.
- R4. **A flaky new two-peer test.** Loopback ICE plus a post-handshake channel open can be slow on
  a loaded host. Mitigation: reuse the existing harness's timeout scale (15s connect, 10s open) and
  assert on delivered bytes rather than on elapsed time.
- R5. **Silent scope creep into channel routing.** Mitigation: the non-scope list above, and the
  invariant that the single-claim rejection policy is unchanged in this ticket.
- R6. **Behavior change from `async_channel`-backed primitives.** `0.21` channels are MPMC
  (`async_channel`) where `0.20` used tokio mpsc. Hub uses single-consumer patterns throughout, so
  semantics hold; Implement confirms no site clones a `Receiver`.

## Acceptance checks and tests

Recorded against a stable commit with a clean tracked worktree.

- A1. `cargo check --workspace --all-targets` passes with zero warnings introduced by this change.
- A2. `./test.sh` (the repository wrapper, `BOTSTER_ENV=test cargo test --workspace`) passes.
- A3. Focused WebRTC lifecycle tests pass:
  `./test.sh --test hub_daemon_lifecycle_test` and the `local_webrtc` module tests, including
  `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline`.
- A4. **New two-peer regression test.** Both peers reach `RTCPeerConnectionState::Connected`. Only
  then does one peer call `create_data_channel` with `ordered: true`, `max_retransmits: None`, and
  `max_packet_life_time: None`. The test asserts, in order:
  1. the remote peer receives `on_data_channel` for that label;
  2. the creating side observes `DataChannelEvent::OnOpen` on the late channel;
  3. a payload sent on the late channel arrives byte-identical on the remote side.
  Assertion 3 is the load-bearing one: an open event alone does not prove delivery.
- A5. **Red-on-revert ablation.** With `webrtc` pinned back to `0.20.0-beta.2`, the A4 test must
  fail (the late channel does not open within the bound). Implement records the ablation output in
  the report. This is the control that proves the test measures the upgrade and not the harness.
- A6. **Detach-on-drop assertion.** A test proves a task spawned through `Runtime::spawn` whose
  `Box<dyn JoinHandle>` is dropped still runs to completion. This closes the teardown risk that a
  silently cancelled poller would create.
- A7. **Preserved fail-closed production proof.** The existing extra-DataChannel rejection tests
  pass unchanged through `LocalWebrtcHandler::on_data_channel`, and the surviving-channel positive
  control still observes expected payload traffic on the surviving channel before asserting zero
  frames on the rejected channel.
- A8. **Preserved lifecycle proof.** Pre-handshake channel creation, signaling, ICE gathering,
  AES-GCM encryption, chunking, close bounds, reconnect, and peer lifecycle tests all pass with no
  test weakened, no assertion deleted, and no bound widened in production code.
- A9. **Exact version evidence.** The implement report records the resolved `webrtc`, `rtc`, and
  `rtc-*` versions from `Cargo.lock`, the base commit, and the `Cargo.lock` diff summary.

### Downstream proof

No downstream repository proof is required by the charter for this ticket. `webrtc` is a Hub-root
dependency, and no published DTO, protocol revision, capability token, or package version changes.
The downstream consumers of this capability are the two Hub tickets named above, and they carry
their own live-proof requirements.

## Vault gaps worth capturing

- G1. *`webrtc 0.20` does not flush DCEP for a post-handshake DataChannel.* The `wake_writes()`
  addition in `0.21` is the reason browser-created subscription channels were not possible before.
  This is exactly the kind of upstream fact a future planner would otherwise re-derive from a crate
  diff.
- G2. *The `webrtc 0.21` runtime migration shape.* Free `block_on` / `sleep` / `timeout` become
  runtime-bound, `timeout` returns `Elapsed`, `block_on` is restricted to `()` output for object
  safety, and the state and event enums become `#[non_exhaustive]`. Worth capturing once so the
  next Hub dependency roll does not rediscover it.
- G3. *A dependency-capability ticket needs a red-on-revert ablation, not a production-path claim.*
  This plan's split between library-level positive proof and production-level preserved fail-closed
  proof is a reusable pattern for upgrades that unlock a capability the product does not consume yet.

Capture after Verify, not during Plan.
