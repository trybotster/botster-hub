# Plan: upgrade WebRTC for post-handshake DataChannel creation

Ticket: `ticket_1787654915_646236`
Run: `run_1787654940_337274`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Plan base commit: `f66d459` (clean tracked worktree)

## Plan Review response (review_1787658403_670109, changes_required) — rev4

One finding, medium severity, and it was correct. The reviewer did not just read the plan — they ran
the broad `peer_close` command against `hub_daemon_lifecycle_test` on a refreshed base and reported
what it actually selected. That is the right way to test an acceptance claim.

| Finding | Severity | Resolution |
|---------|----------|------------|
| The new and Hello-sweep proofs still lack exact selectors | medium | Accepted in full. Rev3 deferred the A4, A4-live, and Hello-sweep test names to Implement, which left three of the plan's own claims without a checkable command. Stable names are now chosen in the plan, every command is complete with `-- --exact`, and each states its expected `running N tests` / `N passed` count. The broad `peer_close` command is removed as sweep proof. |

Two things the finding surfaced that go beyond renaming:

1. **The broad filter was not merely imprecise, it was empty of the claimed proof.** `peer_close`
   selects `peer_close_leaves_sibling_peers_working`,
   `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route`, and
   `local_webrtc_peer_close_detaches_terminal_subscriptions`. None asserts removal from
   `pending_runtime.webrtc_admissions` or `pending_runtime.host_compatibility`. Rev3 would have let
   a reviewer accept three passing tests as proof of an invariant none of them checks.
2. **The sweep cannot be proven from the lifecycle suite at all.** `pending_runtime` is internal
   state with no live oracle, so no integration test can assert it. A9 now places the proof in a
   library unit test using the existing `PeerHarness`, which owns `harness.state` and calls the real
   `handle_control_message`. Naming the right home was the substantive part of this fix; renaming
   was the easy part.

Unknown U5 is resolved at plan time rather than deferred, since Plan Review's run answered it.

## Plan Review response (review_1787657378_172980, changes_required) — rev3

Second review. All three findings carried concrete `details` and `suggested_fix`, and all three were
correct. Each was verified against the code before the fix.

| Finding | Severity | Resolution |
|---------|----------|------------|
| The `Hello` row omits live admission ownership and its cleanup | high | Accepted; my rev2 row was factually wrong. Verified in code: `local_webrtc.rs:1440` sends `RegisterWebrtcAdmission`, `daemon_transport.rs:2452` inserts a `HostCompatibilityRecord` and a `WebrtcTerminalAdmission` keyed by `grant_id`, an `Admitted` entry carries the terminal-route `WebRtcConnectionMux` and binds `close_work`, the insert is guarded by `has_live_peer` at `:2457`, and `LocalWebrtcPeerClosed` removes both rows at `:3046-3047`. The matrix row now states all of that, and A9 proves the sweep. |
| The red-on-revert rule conflicts with the stated live Hub risk | high | Accepted. Rev2 required both A4 and A4-live to go red on `0.20` while R6 said A4-live might legitimately stay green — those cannot both be one pass gate. A5 now makes **A4 the causality gate** (the library test carries no unrelated traffic, so nothing else can wake the driver), treats the **A4-live `0.20` result as diagnostic**, and adds an explicit escalation: if A4 stays green on `0.20`, assumption A2 is false and Implement must call `project_pipelines_ask_human` rather than decide alone. |
| Focused WebRTC acceptance lacks exact selectors and nonzero counts | medium | Accepted. A3 now lists six exact commands, A9 adds the Hello-sweep selector, and A9b covers the three new or hardened tests. Every command must be recorded with its `running N tests` line and **N nonzero**, because a filter that selects zero tests passes vacuously. The `webrtc_peer_rejects_a_second_data_channel` prefix must report two tests, since a count of one means its one-shot-claim negative control never ran. |

No finding was disputed in this round.

## Plan Review response (review_1787656724_548895, changes_required) — rev2

This revision answers all five findings. The findings carried no `details` or `suggested_fix`, so
each was re-derived from the repository and the vault.

| Finding | Severity | Resolution |
|---------|----------|------------|
| The live Hub proof does not prove post-handshake DataChannel arrival | high | Accepted. Added acceptance check **A4-live**: a live isolated Hub daemon, a channel created after the encrypted `Hello`, and the existing Hub-side observation oracle (`lost_claim`, `close_ok`, `label`) plus close marker. This proves DCEP reaches the production `LocalWebrtcHandler::on_data_channel` post-handshake, with no reservation or routing work. See *production_path_proof*. |
| The late-message admission matrix omits ownership-creating request surfaces | high | Accepted. The matrix now covers `Hello`, `Attach`, `Detach`, `SubscribeEntities`, `UnsubscribeEntities`, `SubscribeEvents`, `UnsubscribeEvents`, `Spawn`/`ShutdownSession`, and `PeerClosed`, each with owner tag, rejection, and sweep, grounded in the `LocalWebrtcPeerState` owner set. |
| The acceptance sequence omits required fresh-target prebuild gates | high | Accepted. Added the *Fresh-target prebuild preconditions* block: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, then `cargo build --locked --bin botster-hub`, then `./test.sh --locked`. A `Cargo.lock` change forces a fresh target, so this applies. |
| The plan does not record required Botster architecture and runtime guidance | medium | Accepted. Loaded and recorded [[botster-architecture]], [[cli-patterns]], [[botster-runtime-reviewer-playbook]], and [[botster-runtime-verifier-playbook]], plus the atomic notes now cited in the matrix and acceptance checks. |
| Plan completion evidence and the vault checklist are missing | medium | Partly a reporting defect on my side. The gate result `gate_result_1787655633_511297` did carry every required field, and checklist `checklist_1787655432_135912` exists — but the vault checklist was created with `scope: "ticket"` under a run `owner_id`, so it appears in neither `run_checklists` nor `ticket_checklists`, and the `step.completed` event recorded `evidence: {}`. This revision passes the same evidence to `request_step_advance` so it lands on the step record, and states the checklist id and its scope quirk explicitly. No second checklist was created. |

One finding is worth flagging back rather than silently absorbing: chasing A4-live exposed a latent
defect in an existing test. See the note under *production_path_proof* about the discarded open
signal in `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames`.

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

Required Botster architecture and runtime guidance (added after Plan Review finding 4):

- [[botster-architecture]] -- Botster domain map and source of architectural truth.
- [[cli-patterns]] -- Rust CLI, TUI, PTY, and terminal-layer constraints.
- [[botster-runtime-reviewer-playbook]] -- the review overlay this daemon and transport diff will be
  checked against, loaded at Plan so the acceptance checks match what Review will demand.
- [[botster-runtime-verifier-playbook]] -- the Verify overlay, loaded at Plan so the live-proof
  obligations are designed in rather than retrofitted.

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
- [[webrtc peer cleanup removes every per peer owner together]]
- [[Client event holders are connection-scoped]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]

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
6. Add one live Hub test that proves a post-handshake DataChannel reaches the production
   `LocalWebrtcHandler::on_data_channel` and is rejected fail-closed, using the existing Hub-visible
   observation oracle.
7. Require the post-handshake open before the existing zero-terminal-frame isolation assertion,
   which today can pass because the channel never opened.
8. Record exact dependency versions and migration evidence in the implement report.

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
| `src/local_webrtc.rs` | `timeout` call sites gain the runtime argument; the `RTCPeerConnectionState` match at line 1109 gains a fail-safe arm; two library tests are added to the existing in-file harnesses — `post_handshake_data_channel_opens_and_delivers_bytes` (A4) and `peer_closed_removes_webrtc_admission_and_host_compatibility` (A9, using `PeerHarness`) |
| `src/local_webrtc_smoke.rs` | import list; one `block_on`, one `sleep`, and the `timeout` call sites |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | import list; two `block_on` sites and the `timeout` call sites |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | the new live Hub post-handshake arrival test `webrtc_peer_post_handshake_data_channel_reaches_production_reject` (A4-live), reusing `start_webrtc_adapter_hub_with_env` and the existing observation oracle |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | require the post-handshake open before the zero-terminal-frame assertion (A7) |
| `docs/reports/upgrade-webrtc-for-post-handshake-datachannel-creation-implement.md` | migration evidence, exact versions, and separate Hub / locked-Core provenance |

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

`late_message_matrix`: Every control-plane surface that creates durable per-peer ownership is
listed, not only the DataChannel open. The per-peer owner set is explicit in
`LocalWebrtcPeerState`: `attached_subscriptions`, `entity_subscription_ids`, the connection-scoped
event-plane holders, the `WebRtcConnectionMux` routes, and the `data_channel_claimed` one-shot.

| Surface | Owner tag | Rejection after terminal failure | Residual sweep |
|---------|-----------|----------------------------------|----------------|
| `on_data_channel` (first channel) | `data_channel_claimed` one-shot under `grant_id` | peer terminal cause published; poller exits | peer cleanup removes every per-peer owner together |
| `on_data_channel` (post-handshake / extra) — newly reachable | claim already taken, so `claimed == false` | `reject_extra_data_channel` closes it under the peer close bound; no route, no adapter bound | none: nothing was inserted |
| `Hello` (terminal admission) | `grant_id`. `local_webrtc.rs:1440` sends `ControlMessage::RegisterWebrtcAdmission`; `daemon_transport.rs:2452` inserts a `HostCompatibilityRecord` into `pending_runtime.host_compatibility` and a `WebrtcTerminalAdmission` into `pending_runtime.webrtc_admissions`, both keyed by `grant_id`. An `Admitted` admission carries the `WebRtcConnectionMux` used for terminal routes and binds `close_work` into it. | the handler inserts only `if daemon.local_webrtc().has_live_peer(&grant_id)` (`daemon_transport.rs:2457`), so an admission racing `PeerClosed` is dropped rather than resurrected | `LocalWebrtcPeerClosed` removes both rows for every entry in `removed_grants` (`daemon_transport.rs:3046-3047`), and the admitted mux closes with the peer as part of the single peer cleanup |
| `Attach` | `grant_id` + `(session_id, subscription_id, generation)` in `attached_subscriptions` | rejected after terminal failure; pre-READY failure creates no ownership | peer cleanup drains `attached_subscriptions` and releases route occupancy |
| `Detach` | same attach identity | idempotent after terminal failure | route-aware and idempotent cleanup |
| `SubscribeEntities` | `grant_id` + subscription id in `entity_subscription_ids` | rejected after terminal failure | peer cleanup clears `entity_subscription_ids` |
| `UnsubscribeEntities` | same subscription id | idempotent | no residual |
| `SubscribeEvents` / `UnsubscribeEvents` | connection-scoped event holder under the WebRTC grant owner | rejected after terminal failure | event-plane holders retire with the connection |
| `Spawn` / `ShutdownSession` | session-owned, not peer-owned | unchanged | session lifecycle owns teardown |
| `PeerClosed` | `grant_id` | terminal | sweeps the whole owner set in one cleanup, per [[webrtc peer cleanup removes every per peer owner together]] |

Correction from Plan Review (finding_1787657378_238601): revision 2 of this plan claimed the
`Hello` row "never becomes a route" and listed no sweep. That was wrong. Hello admission creates two
durable rows keyed by `grant_id`, and the admitted entry carries the terminal-route mux. The row
above now names both rows, the live-peer guard that rejects a late admission, and the `PeerClosed`
sweep that removes them. Acceptance check A9 proves the sweep.

Invariant this ticket must preserve: **no row in this table changes.** The upgrade adds no owner
tag, no rejection rule, and no sweep. The only row whose *arrival timing* changes is the
post-handshake `on_data_channel` row, and it stays fail-closed. The reservation-tagged rows arrive
with `ticket_1787600674_500120`.

`production_path_proof`: The production path is
`browser creates a DataChannel after Connected -> the driver flushes DCEP OPEN (the 0.21
wake_writes fix) -> LocalWebrtcHandler::on_data_channel -> claim_data_channel() returns false ->
reject_extra_data_channel -> bounded local_close -> the peer keeps serving its first channel`.

This path is provable live today, through the real Hub daemon, without any reservation or routing
work. Two pieces already exist in the repository and this plan composes them:

1. `webrtc_fixtures.rs::create_extra_data_channel` creates a DataChannel on an already-connected
   peer. `webrtc_terminal_adapter.rs` already calls it against a live isolated Hub.
2. `local_webrtc.rs::observe_rejected_data_channel_for_test` writes a Hub-side observation file
   (`lost_claim`, `close_ok`, `label`) plus a close marker from the **production**
   `on_data_channel` -> `reject_extra_data_channel` path, gated on `BOTSTER_ENV=test` and the
   `BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION` / `..._CLOSE_MARKER` environment values, which
   `start_webrtc_adapter_hub_with_env` sets on the Hub child. The existing
   `webrtc_peer_rejects_a_second_data_channel` test proves this oracle works — but it creates both
   channels in the initial offer, so it proves only the **pre-handshake** case.

Acceptance check A4-live below closes the gap: the same Hub-visible oracle, driven by a channel
created **after** the handshake. The observation file can only appear if the DCEP OPEN actually
reached the production Hub handler post-handshake, which is precisely the behavior `wake_writes()`
enables. That is a live Hub production-path proof, not a terminal record and not a helper call, as
[[terminal webrtc failure records do not prove peer runtime teardown]] requires.

A latent defect this exposes, recorded because the upgrade causes it: today
`webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames` discards the open
signal (`let _ = timeout(5s, extra_channel.open_rx.recv())`) and then asserts the extra channel
received zero terminal frames. On `0.20` a channel that never opens satisfies that negative
assertion for the wrong reason. Once the channel really opens, the assertion becomes meaningful for
the first time, so acceptance check A7 requires the open before counting zero frames. This is
cleanup made necessary by this change, not adjacent cleanup, and it is exactly the surviving-channel
positive control [[rejected channel isolation needs a surviving channel positive control]] demands.

Scope boundary that still holds: Hub continues to **reject** the post-handshake channel. Positive
live proof of an *admitted* browser-created subscription channel remains the deliverable of
`ticket_1787600674_500120`. This plan proves arrival and fail-closed handling, and names the
admission proof rather than claiming it.

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
- U5. RESOLVED at plan time, not deferred to Implement. Whether any existing test asserts that both
  `pending_runtime.webrtc_admissions` and `pending_runtime.host_compatibility` rows are removed on
  `PeerClosed`: Plan Review ran the broad `peer_close` filter on a refreshed base and it selected
  three tests, none of which asserts either removal. No existing test covers the sweep, so
  acceptance check A9 names the new library test that must be added and gives its exact command.

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
- R6. **Live Hub traffic can mask the dependency change.** The Hub's own `Hello`, `Spawn`, and
  `Attach` traffic can wake the driver, so A4-live may pass on `0.20` for reasons unrelated to
  `wake_writes()`. This is why A5 makes A4, not A4-live, the causality gate, and treats the A4-live
  `0.20` result as diagnostic. If A4 itself stays green on `0.20`, assumption A2 is false and the
  escalation rule in A5 applies: stop and ask a human.
- R7. **Fresh-target suite failures mistaken for regressions.** The `Cargo.lock` change forces a
  fresh target, where missing-worker failures look like real breakage. Mitigation: the two prebuild
  commands are a stated precondition, not an optimization.
- R8. **Behavior change from `async_channel`-backed primitives.** `0.21` channels are MPMC
  (`async_channel`) where `0.20` used tokio mpsc. Hub uses single-consumer patterns throughout, so
  semantics hold; Implement confirms no site clones a `Receiver`.

## Acceptance checks and tests

Recorded against a stable commit with a clean tracked worktree.

### Fresh-target prebuild preconditions (required before the wrapper)

Per [[Hub suite runs prebuild the session worker before the locked test wrapper]] and the charter
rule "Before `./test.sh --locked` on a fresh target, build `botster-session-worker` and then build
`botster-hub` with locked commands", this run changes `Cargo.lock` and therefore forces a fresh
target. Lazy worker discovery is not sufficient; the prebuild is a suite precondition, not an
optimization. Run in this order:

```bash
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
./test.sh --locked
```

Per [[live hub proof records distinct hub and locked core binary provenance]], the implement report
records the Hub commit and the lockfile-pinned Core revision separately, and resolves fresh-target
realpaths for both binaries.

### Checks

- A1. `cargo check --workspace --all-targets` passes with no warnings introduced by this change.
- A2. `./test.sh --locked` passes, after the two prebuild commands above.
- A3. **Focused WebRTC lifecycle proofs, by exact selector.** Every command below must be recorded
  in the implement report with its `running N tests` and `N passed` lines, and **N must match the
  stated expectation**. A filter that selects zero tests passes vacuously and is not evidence.

  ```bash
  # Close bound and fail-closed handler deadline — expect: running 1 test, 1 passed
  BOTSTER_ENV=test cargo test --locked --lib \
    local_webrtc::tests::local_webrtc_close_hang_fail_closed_returns_handler_within_deadline \
    -- --exact
  # Ultimate-close sibling sacrifice and full owner sweep — expect: running 1 test, 1 passed
  BOTSTER_ENV=test cargo test --locked --lib \
    local_webrtc::tests::ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners \
    -- --exact
  # Single-peer failure preserves siblings and the runtime — expect: running 1 test, 1 passed
  BOTSTER_ENV=test cargo test --locked --lib \
    local_webrtc::tests::local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime \
    -- --exact
  # Live sibling survival across peer close — expect: running 1 test, 1 passed
  ./test.sh --locked --test hub_daemon_lifecycle_test \
    peer_close_leaves_sibling_peers_working -- --exact
  # Post-handshake isolation, with the open now required (A7) — expect: running 1 test, 1 passed
  ./test.sh --locked --test hub_daemon_lifecycle_test \
    webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames -- --exact
  # Pre-handshake rejection proof AND its negative control — expect: running 2 tests, 2 passed
  ./test.sh --locked --test hub_daemon_lifecycle_test webrtc_peer_rejects_a_second_data_channel
  ```

  The last command is deliberately **not** `--exact`: it is a prefix match that must select both
  `webrtc_peer_rejects_a_second_data_channel` and its `..._requires_one_shot_claim` negative
  control. A count of one means the negative control did not run and the evidence is incomplete.

- A4. **Two-peer library regression test.** Both peers reach `RTCPeerConnectionState::Connected`.
  Only then does one peer call `create_data_channel` with `ordered: true`, `max_retransmits: None`,
  and `max_packet_life_time: None`. The test asserts, in order:
  1. the remote peer receives `on_data_channel` for that label;
  2. the creating side observes `DataChannelEvent::OnOpen` on the late channel;
  3. a payload sent on the late channel arrives byte-identical on the remote side.
  Assertion 3 is load-bearing: an open event alone does not prove delivery.
- A4-live. **Live Hub post-handshake arrival proof.** Through a real isolated Hub daemon started by
  `start_webrtc_adapter_hub_with_env` with `BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION` and
  `BOTSTER_HUB_TEST_EXTRA_CHANNEL_CLOSE_MARKER`: connect, complete the encrypted `Hello` so the
  one-shot claim is taken, and only then call `create_extra_data_channel()`. Require all of:
  1. the offerer observes `OnOpen` on the post-handshake channel (the open result must be asserted,
     not discarded);
  2. the Hub writes the observation file with `lost_claim == true`, `close_ok == true`, and
     `label == "botster-extra"`, proving the DCEP OPEN reached the **production**
     `LocalWebrtcHandler::on_data_channel` after the handshake;
  3. the close marker exists, proving the bounded `local_close` completed.
  This is the live Hub production-path proof. It uses the existing Hub-visible oracle rather than a
  helper call, and it requires no reservation or routing work.
- A5. **Red-on-revert ablation**, per [[a regression test must be shown to go red with the fix
  reverted]]. Revision 2 required both A4 and A4-live to fail on `0.20`, which contradicted risk R6.
  Plan Review was right that those cannot both define one pass gate. The rule is now split by what
  each test can actually isolate:
  - **A4 is the required dependency-causality ablation.** The two-peer library test carries no
    unrelated traffic, so nothing else can wake the driver. With `webrtc` pinned to `0.20.0-beta.2`
    it **must** fail. This is the gate.
  - **A4-live is the required production-path proof on `0.21` only.** A live Hub necessarily carries
    `Hello`, `Spawn`, and `Attach` traffic that can wake the driver on its own, so its `0.20` result
    is **diagnostic, not a gate**, unless the test is built to isolate unrelated driver wakes.
    Implement records the `0.20` A4-live result either way.
  - **Escalation rule.** If A4 also stays green on `0.20`, the ticket's stated dependency rationale
    is false. Implement must stop and call `project_pipelines_ask_human` rather than proceed. The
    migration may still be worth doing, but that is a human decision, not a planner's.
- A6. **Detach-on-drop assertion.** A task spawned through `Runtime::spawn` whose
  `Box<dyn JoinHandle>` is dropped still runs to completion, closing the risk that a silently
  cancelled poller changes teardown behavior.
- A7. **Surviving-channel positive control, now meaningful.** In
  `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames`, require the
  post-handshake channel to open before asserting it received zero terminal frames, and keep the
  existing proof that the surviving admitted channel does receive `DaemonTerminalFrame` chunks in
  the same window. Today the open result is discarded, so on `0.20` the zero-frame assertion can
  pass because the channel never opened. This ordering is required by
  [[rejected channel isolation needs a surviving channel positive control]].
- A8. **Preserved lifecycle proof.** Pre-handshake channel creation, signaling, ICE, AES-GCM
  encryption, chunking, close bounds, reconnect, and peer lifecycle tests all pass with no test
  weakened, no assertion deleted, and no production bound widened.
- A9. **Hello admission sweep proof, by exact selector and stable name.** The corrected matrix row
  claims that `LocalWebrtcPeerClosed` removes both `pending_runtime.webrtc_admissions` and
  `pending_runtime.host_compatibility` for the closed `grant_id`. Plan Review ran the broad
  `peer_close` filter against `hub_daemon_lifecycle_test` on a refreshed base: it selects **three**
  tests (`peer_close_leaves_sibling_peers_working`,
  `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route`, and
  `local_webrtc_peer_close_detaches_terminal_subscriptions`) and **none** asserts either removal.
  That broad command is therefore removed as sweep proof.

  The sweep cannot be proven from the lifecycle suite, because `pending_runtime` is internal state
  with no live oracle. It belongs in a library unit test, where the existing `PeerHarness` in
  `src/local_webrtc.rs` tests owns `harness.state` and calls the real `handle_control_message`.
  Implement adds one test with this stable name:

  `local_webrtc::tests::peer_closed_removes_webrtc_admission_and_host_compatibility`

  It drives `RegisterWebrtcAdmission` for a live grant, asserts both rows are present, then drives
  `LocalWebrtcPeerClosed` for that grant and asserts both rows are gone.

  ```bash
  # expect: running 1 test, 1 passed
  BOTSTER_ENV=test cargo test --locked --lib \
    local_webrtc::tests::peer_closed_removes_webrtc_admission_and_host_compatibility -- --exact
  ```

- A9b. **Stable names and exact selectors for the two new tests.** Implement uses these names; they
  are chosen here, not deferred, so Review can check the selector against the diff.

  ```bash
  # A4 two-peer library regression test — expect: running 1 test, 1 passed
  BOTSTER_ENV=test cargo test --locked --lib \
    local_webrtc::tests::post_handshake_data_channel_opens_and_delivers_bytes -- --exact
  # A4-live production arrival proof (new, in subscription_ownership_baseline.rs)
  # expect: running 1 test, 1 passed
  ./test.sh --locked --test hub_daemon_lifecycle_test \
    webrtc_peer_post_handshake_data_channel_reaches_production_reject -- --exact
  ```

  If Implement must rename either test, it records the final name and the exact command it ran, and
  says why the planned name did not fit. Renaming is allowed; omitting the exact command is not.

- A10. **Exact version evidence.** The implement report records the resolved `webrtc`, `rtc`, and
  `rtc-*` versions from `Cargo.lock`, the base commit, the separate Hub and locked-Core provenance,
  and the `Cargo.lock` diff summary.

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
