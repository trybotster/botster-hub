# Freeze subscription ownership and capture the regression baseline

Plan for ticket `ticket_1787600670_129312` in project `project_1787600579_585482`
(Botster Isolated Subscription Data Plane), pipeline `botster_stack_delivery`,
run `run_1787605830_934897`, step `botster_stack_plan`.

This plan is the executable architecture contract for the project. Downstream
tickets cite this document by section number.

## 1. Target

| Field | Value |
|-------|-------|
| Target repository | `botster-hub` (`https://github.com/trybotster/botster-hub.git`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Base commit | `85a0434`, equal to `origin/main` |
| Plan commit | The single squashed commit that adds this document. `origin/main..HEAD` contains exactly one commit; verify with `git rev-list --count origin/main..HEAD`, which must return 1. The Plan gate evidence records the exact SHA. The plan artifact records the plan URI. This document cannot contain its own commit hash. |
| Worktree | clean, tracked `.gitignore` present (5 lines), path contains no `:` |
| Locked Core revision | `7eafa470a18025895995bbedc20d34b58106a03b` (`botster-core`, `botster-core-daemon`, `botster-terminal-protocol`, `botster-core-test-support`, `botster-terminal-ghostty`) |

The `target_id` was resolved from the run record, not from the process working
directory. The advisor answer for `question_1787603862_971401` confirms the same
mapping.

Commit identity is squashed on purpose. Earlier revisions of this plan shipped as
`69688b5` and then `41650c3`, which left §1 naming `69688b5` as "the only commit"
while the reviewed tip was `41650c3`. A plan that carries a stale self-reference
is worse than one that carries none, so the branch is now a single commit and §1
points at the gate evidence for the exact SHA instead of restating it.

The ticket branch previously carried `51a2ac9`, whose message reads "Update locked
Botster Core revision". Its diff touches only transitive `windows-sys` and
`getrandom` entries in `Cargo.lock` and changes no Core revision. It belongs to no
ticket in this project. The branch was rebased onto `85a0434` to drop it, so
`git diff 85a0434 -- Cargo.lock` is now empty and every measurement in this plan
sits on a clean ticket base.

## 2. Playbooks and notes loaded

Repository playbook: `[[botster-hub-playbook]]`.

Role playbooks, in load order:

1. `[[planner-playbook]]`
2. `[[botster-planner-playbook]]`
3. `[[botster-hub-playbook]]`
4. `[[botster runtime teardown lenses]]` (runtime-teardown class applies, see §11)

Required `[[botster-planner-playbook]]` context, loaded on the Plan Review return:

- `[[botster-architecture]]` — the domain map and source of architectural truth.
- `[[cli-patterns]]` — Rust CLI, TUI, PTY, and terminal-layer constraints.
- `[[spa-patterns]]` — React SPA and entity-store frontend constraints.

The first plan omitted these three. They are not decorative here: `[[spa-patterns]]`
supplies the reconnect and pull-replay contract that §14.1 now tests, and
`[[cli-patterns]]` supplies the release-chain and admission-evidence rules that
§14.2 now tests. Both changed the acceptance matrix.

Atomic notes read for this plan:

- `[[core owns duplex terminal transport while Hub stays content blind]]`
- `[[botster subscriptions use dedicated ordered DataChannels]]`
- `[[Hub extraction must reduce ownership rather than only split files]]`
- `[[botster hub gravity must be watched before it becomes the new monolith]]`
- `[[botster Hub Rust stays a trusted host kernel]]`
- `[[botster hub is a first party host profile over core]]`
- `[[botster data plane bypasses the hub through session and client actors]]`
- `[[lua plugins are the hub composition layer]]`
- `[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`
- `[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`
- `[[a ready WebRTC send must win over a queued DataChannel close]]`
- `[[Client event holders are connection-scoped]]`
- `[[admitted event holders survive producer unload until Core completion]]`
- `[[terminal transport north star publishes behavioral oracles not numeric budgets]]`
- `[[Fair host-control writing selects already-admitted frames]]`
- `[[a page reload is not a reconnect]]`
- `[[botster browser pull requests must retry after webrtc reconnect]]`
- `[[closed dependency tickets signal merged source not a consumable release]]`
- `[[hub generated protocol changes are a four site release chain]]`
- `[[WebRTC adapter admission uses a Hello feature string not a generated DTO token]]`
- `[[live hub proof records distinct hub and locked core binary provenance]]`
- `[[Hub suite runs prebuild the session worker before the locked test wrapper]]`
- `[[a regression test must be shown to go red with the fix reverted]]`

`[[project-pipelines-playbook]]` was not loaded. This ticket changes no Project
Pipelines package or plugin path.

## 3. Context loaded

Source read in the target repository at `85a0434`:

- `src/local_webrtc.rs` (7,219 lines) — signaling, grant, peer state, single
  DataChannel loop, AES-GCM framing, chunking, host-control fair write.
- `src/daemon_transport.rs` (10,563 lines) — Unix accept and framing, owner
  loop, control request dispatch, terminal JSON handlers, Unix mux writer,
  admission records, attach occupancy.
- `src/webrtc_terminal_adapter.rs` (926 lines) — `WebRtcTerminalAdapter`,
  `WebRtcConnectionMux`, route map keyed `(session_id, subscription_id, generation)`.
- `src/unix_terminal_adapter.rs` (915 lines) — the Unix equivalent.
- `src/host_control_fair_write.rs` (130 lines) — `HostControlClass` rotation.
- `src/client_api.rs`, `src/runtime.rs`, `src/lua_runtime.rs`, `src/config.rs`.
- `crates/botster-hub-client/src/lib.rs` — `DaemonRequest` vocabulary.

Pinned Core contract read at `7eafa47`:
`crates/botster-core/src/contract/terminal_adapter.rs`.

CI read: `.github/workflows/ci.yml`, `.github/workflows/loaded-daemon-lifecycle.yml`
(line 69 already selects `botster-ubuntu-24.04-16core` for the
`event-plane-saturation` target).

Pipeline context read: project description, all twelve project tickets, the three
prior advisor answers, and the open-ticket list across all projects.

## 4. Scope

In scope for this ticket:

1. The responsibility map of §5.
2. The Lua hot-path proof of §6.
3. The shared-queue and shared-DataChannel proof of §7.
4. The frozen channel contract of §8 — topology, creator, label scheme,
   encryption, chunking.
5. The exact limit table of §9.
6. The ownership contract of §10 — Core, Hub Rust, Lua, entity, event.
7. The runtime-teardown lens answers of §11.
8. The extraction map of §12 — target module and owning ticket per responsibility.
9. The symbol classification of §13 — rewrite, delete, unchanged.
10. The acceptance matrix of §14 and the Hub characterization tests of §15.

Non-scope:

- Do not redesign Ghostty terminal semantics.
- Do not perform Web timing measurements. `ticket_1787603669_760394` owns the
  pre-change observation set and publishes the observation format.
- Do not change transport behavior in this ticket. This ticket adds the contract
  document and the characterization tests that pin current behavior.
- Do not delete `DaemonRequest::SendInput`, `ModeGatedInput`, or `Resize`.
  `ticket_1787600679_990088` owns that deletion after every consumer has moved.
- Do not plan extractions for responsibilities this project does not migrate.
  `src/runtime.rs`, `src/packages.rs`, `src/main.rs`, `src/daemon_maintenance.rs`,
  and `src/package_event_router.rs` receive no extraction assignment.
- Do not change the Unix entity subscription stream
  (`handle_entity_subscription_async`). This project moves the WebRTC entity
  plane only.

## 5. Current responsibility map

### 5.1 Core (`botster-core` at `7eafa47`)

Core owns terminal sessions, attach phases, snapshots, ordering, generations,
bounded queues, pressure, and teardown. `ClientWorker` writes egress through
`TerminalAdapter::try_write`. The published contract is **egress only**:
`try_write`, `close`, `pressure`. There is no ingress method. Terminal input
therefore cannot reach Core through the adapter today.

Core already separates attach readiness from history completion
(`snapshot_delivery=ready_then_history`).

### 5.2 Hub Rust

Hub owns admission, signaling, grants, persistence, supervision, package policy,
plugin isolation, and the two transport hosts (Unix, WebRTC). Hub also owns, today,
work that belongs to Core or to a subscription channel:

- Terminal input, mode-gated input, and resize as JSON control requests
  (`src/daemon_transport.rs:3834`, `:3849`, `:3874`).
- A single shared WebRTC DataChannel that multiplexes control, entity, event, and
  terminal frames (`src/local_webrtc.rs:1208` `run_data_channel`).
- A cross-lane fair-write scheduler over that shared channel
  (`src/host_control_fair_write.rs`, used at `src/local_webrtc.rs:2007-2073`).

### 5.3 Lua plugins

Lua composes commands, hooks, UI actions, entity providers, MCP tools, timers,
and package events. See §6 for the hot-path proof.

### 5.4 Web, TUI, WebRTC, Unix, Restty

- Web reaches Hub over one WebRTC peer and one DataChannel, and sends terminal
  input as JSON `SendInput` / `ModeGatedInput` requests.
- TUI reaches Hub over the Unix socket and sends the same JSON requests
  (`crates/botster-hub-client/src/lib.rs:1081`, `:1088`).
- WebRTC transport lives in `src/local_webrtc.rs` plus
  `src/webrtc_terminal_adapter.rs`.
- Unix transport lives in `src/daemon_transport.rs` plus
  `src/unix_terminal_adapter.rs`.
- Restty is vendored inside `botster-web`. Hub holds no Restty responsibility.
  `ticket_1787600689_646958` owns the Restty revision.

## 6. Lua hot-path proof

Claim: no Lua code runs in terminal attach, input, or output paths today.

Evidence:

1. Hub imports `crate::lua_runtime` from exactly two modules: `src/runtime.rs:52`
   and `src/lib.rs:166`.
2. Lua handler dispatch has three entry points:
   - `HubRuntime::dispatch_plugin_surface_action` (`src/runtime.rs:2938`), called
     only from `src/client_api.rs:657` for `PluginHandlerKind::UiAction`.
   - Lifecycle and package-event dispatch of `PluginHandlerKind::Event`
     (`src/lifecycle.rs`, `src/daemon_maintenance.rs`).
   - MCP tool, command, timer, and entity-provider handlers registered in
     `src/lua_runtime.rs`.
3. The terminal input path is
   `DaemonRequest::SendInput` → `handle_runtime_control_request`
   (`src/daemon_transport.rs:3834`) → `HubClientApi::handle_request`
   (`src/client_api.rs:184`) → `HubRuntime::write_bytes`. `ModeGatedInput` and
   `Resize` follow the same shape at `src/client_api.rs:199` and `:221`. None of
   those arms reaches `lua_runtime`.
4. The terminal output path is Core `ClientWorker` → `TerminalAdapter::try_write`
   → `WebRtcTerminalAdapter` / `UnixTerminalAdapter` → socket or DataChannel.
   Neither adapter module references `lua_runtime`.

Conclusion: Lua is already outside the terminal hot paths. The project must
preserve that property, not create it. `ticket_1787600691_401181` re-verifies it
after the cut.

## 7. Shared queue and shared DataChannel proof

### 7.1 WebRTC: one DataChannel carries four classes

`LocalWebrtcPeerState::claim_data_channel` (`src/local_webrtc.rs:883`) is a
one-shot `AtomicBool`. `LocalWebrtcHandler::on_data_channel`
(`src/local_webrtc.rs:1090`) rejects and closes every additional channel. One peer
therefore has exactly one DataChannel today.

`run_data_channel` (`src/local_webrtc.rs:1208`) is a single loop over that one
channel. Inside one iteration it writes, in this order:

1. Host control frames — control responses, entity frames, and host events —
   through `flush_ready_webrtc_host_control` (`:1999`), which rotates
   `HostControlClass::{Control, Entity, Event}` (`:2037`, `:2038`, `:2062`).
2. Terminal adapter frames through `flush_webrtc_adapter_frames` (`:2158`),
   gated behind `pending_entity.is_none() && !host_event_ready(...)`
   (`src/local_webrtc.rs:1245-1246`).

The gate at `:1245` is the concrete coupling the project removes: a ready entity
frame or a ready host event **defers terminal output on the same peer**.

`LocalWebrtcFlowControl` and the DataChannel `bufferedAmount` thresholds
(`LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW` / `_HIGH`, `src/local_webrtc.rs:53-54`) are
per-channel, so today they are per-peer and shared across all four classes. One
slow class raises `bufferedAmount` for every class.

### 7.2 WebRTC: terminal input shares the control request queue

`PendingLocalWebrtcRequest` is a `VecDeque` bounded by
`LOCAL_WEBRTC_PENDING_REQUESTS = 16` (`src/local_webrtc.rs:51`). Terminal input
arrives as `DaemonRequest::SendInput` / `ModeGatedInput` in that deque, is handed
to the owner thread over `DAEMON_CONTROL_QUEUE_CAPACITY = 256`
(`src/daemon_transport.rs:158`), and each keystroke's response is framed and sent
back on the same shared channel. Terminal input therefore waits behind unrelated
command responses.

### 7.3 Unix: one framed socket carries terminal, entity, event, and control

`MuxWriteState` and `PendingMuxClass` (`src/daemon_transport.rs:812`, `:861`)
interleave host responses, host events, and terminal frames on one socket, with
`flush_unix_mux_writes` (`:941`) and `resume_pending_mux_write` (`:1079`) as the
shared writer.

### 7.4 Correction to the prior advisor reading

The advisor answer for `question_1787605283_448552` states that the fair-write
scheduler is "nearly single-class today" because the `Entity` arm at
`src/daemon_transport.rs:1005` breaks. The source shows a different picture at
each call site:

| Call site | `Control` | `Entity` | `Event` |
|-----------|-----------|----------|---------|
| Unix, `src/daemon_transport.rs:972-1005` | active (`:972`) | **inactive** — `Entity \| None => break` (`:1005`) | active (`:983`) |
| WebRTC, `src/local_webrtc.rs:2037-2073` | active (`:2037`) | **active** (`:2038-2045`) | **active** (`:2061-2073`) |

The Unix call site is two-class, not single-class. The WebRTC call site is
three-class. `ticket_1787600682_233928` must therefore remove live `Entity` and
`Event` arms from **both** senders, not tidy a near-dead one. After that removal
only `HostControlClass::Control` remains on both call sites, so the whole file is
deleted with no single-class remnant.

## 8. Frozen channel contract

### 8.1 Topology

- One `RTCPeerConnection` per browser client. Unchanged.
- One reliable ordered DataChannel for Hub control per peer. The browser creates
  it, exactly as today. Unchanged.
- One reliable ordered DataChannel per admitted subscription: terminal, entity, or
  package event.

### 8.2 Channel creator

**Hub creates every subscription DataChannel. The browser creates only the
control channel.**

Rationale: the subscription generation is assigned by Hub at admission and by
Core at bind. A browser cannot know the generation before admission, so a
browser-created subscription channel would have to be renamed or re-bound after
the fact. Hub-created channels also make the limit table fail-closed: an
unadmitted peer cannot allocate a channel at all.

**Creation and bind are two separate steps.** Hub creates the channel at
admission but binds the Core adapter only on the channel's `open` event. Binding
at creation would make the late-open guard of §11.4 unreachable, because the
adapter would already exist before Hub could observe a retirement race.

Route states: `Reserved` → `Bound` → `Retired`. A route is charged against the
§9 limit table from `Reserved`, not from `Bound`, so an unopened channel cannot
be used to exceed the table.

Exact order:

1. Browser sends `Attach`, `SubscribeEntities`, or `SubscribePackageEvents` on the
   control channel.
2. Hub admits or rejects. Rejection returns a typed operator error on the control
   channel, creates no channel, and charges nothing.
3. On admission Hub inserts a `Reserved` route keyed
   `(session_id, subscription_id, generation)` and creates the labeled
   DataChannel. **Hub binds no Core adapter here.**
4. Hub returns the label in the control response immediately. It does not wait
   for `open`, so a slow channel cannot stall the control plane.
5. On the channel's `open` event Hub re-checks three conditions: the route is
   still `Reserved`, its generation is absent from
   `WebRtcMuxInner::suppress_generations`, and the peer is not dying. Only when
   all three hold does Hub bind the Core adapter and move the route to `Bound`.
6. If any check in step 5 fails, Hub closes the channel under the §11.3 bound,
   releases the `Reserved` slot, and binds nothing. No Core detach is needed,
   because no bind occurred.
7. If `open` does not arrive within `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND`
   (5 s production, 200 ms test), Hub closes the channel, releases the
   `Reserved` slot, and emits a typed `subscription_channel_open_timeout` host
   event on the control channel. The browser retries by re-subscribing; Hub does
   not retry on its own.

Both race orders are defined:

| Order | Sequence | Result |
|-------|----------|--------|
| A, open before retire | `open` → bind → later retirement | Normal teardown: suppress generation, bounded channel close, Core detach, route `Retired`. |
| B, retire before open | retirement → `open` arrives late | Step 5 finds the generation suppressed or the route already `Retired`. Hub closes the channel and binds nothing. No Core route is created. |

`LocalWebrtcPeerState::claim_data_channel` becomes "claim the one browser-created
control channel". Additional browser-created channels stay rejected.

### 8.3 Label scheme

Non-negotiated (in-band) channels with a structured label. Negotiated ids are
rejected: they require both peers to agree on an id before creation, which
reintroduces the ordering problem §8.2 removes.

```
bs/1/<kind>/<session_id>/<subscription_id>/<generation>
```

- `bs/1` — scheme name and scheme version. Bumping `1` is a breaking change.
- `<kind>` — `term`, `ent`, or `evt`.
- `<session_id>` — the Core session id for `term`; the literal `-` for `ent` and
  `evt`.
- `<subscription_id>` — the admitted subscription id.
- `<generation>` — decimal `u64`.

Every segment is percent-encoded. The control channel keeps its current label.

For `term`, the triple `(session_id, subscription_id, generation)` is exactly the
existing `WebRtcMuxRoute` key (`src/webrtc_terminal_adapter.rs:317`) and matches
`[[Core terminal subscription ownership is session, subscription, and generation]]`.
For `ent` and `evt`, ownership is `(grant_id, subscription_id, generation)` with
`generation` from a per-grant monotonic counter; `grant_id` is implied by the peer.

A channel whose label generation appears in
`WebRtcMuxInner::suppress_generations` is closed on open and never bound. See
§11.3.

### 8.4 Encryption

**Keep AES-GCM application encryption on every channel, including subscription
channels. Do not fall back to DTLS only.**

Rationale: the AES-GCM key is derived from the local WebRTC bootstrap grant
secret (`secret_stream_key`, `src/local_webrtc.rs:2364`). It binds the channel to
the issued grant, not merely to the DTLS peer. DTLS alone would drop that binding
and weaken local-origin admission.

Change: derive a per-channel key from the grant stream key and the §8.3 label,
so a frame captured on one subscription channel fails authentication when replayed
on another. The control channel keeps the current unmodified key derivation, so
the published bootstrap protocol does not change.

Encrypting an opaque byte string is content-blind. Hub does not parse
`TerminalFrame` bodies. This preserves
`[[core owns duplex terminal transport while Hub stays content blind]]`.

### 8.5 Chunking

Keep chunking, and move it from JSON text to binary DataChannel messages on
subscription channels.

The Rust WebRTC peer receive path is bounded at 16 KiB, and the current code
chunks at `LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES = 12 KiB`
(`src/local_webrtc.rs:50`). Core terminal frames — snapshot pages in particular —
exceed 12 KiB, so chunking cannot be removed. Subscription channels send binary
messages with a fixed 12 KiB payload chunk and a per-channel reassembly sequence.
The control channel keeps text framing and the current
`LOCAL_WEBRTC_MAX_FRAME_BYTES = 64 KiB` ceiling.

## 9. Exact limit table

Every value is an exact byte or frame count. One table, one accounting site.

Per `RTCPeerConnection`:

| Constant | Value | Charged at |
|----------|-------|-----------|
| `MAX_CONTROL_CHANNELS` | 1 | Browser-created channel claim |
| `MAX_SUBSCRIPTION_CHANNELS` | 32 | Route `Reserved` (§8.2 step 3) |
| `MAX_TOTAL_CHANNELS` | 33 | Both of the above |
| `AGGREGATE_BUFFERED_HIGH` | 2 MiB = 2,097,152 B | Every accepted send on a **subscription** channel |
| `AGGREGATE_BUFFERED_LOW` | 1 MiB = 1,048,576 B | Resume threshold for a held class |

#### 9.1 The aggregate excludes the control channel

`AGGREGATE_BUFFERED_HIGH` covers the 32 subscription channels only. The control
channel is outside it and keeps its own limits from the per-channel table.

This is not a refinement; it removes a deadlock. Every overflow action in the
per-channel table reports through the control channel: entity overflow sends a
typed close reason, admission rejection sends a typed operator error, an open
timeout sends `subscription_channel_open_timeout`. If control shared one budget
with the subscription channels, saturating that budget would refuse the very
response that reports the saturation, and the connection would wedge with no
diagnosis. Control is a fixed, small, Hub-authored lane; it is never the cause of
subscription saturation, so it must not be its victim.

With control excluded, the sum of subscription high-water marks is exactly
32 × 128 KiB = 4,194,304 B = 4 MiB. `AGGREGATE_BUFFERED_HIGH` at 2 MiB is half of
that, so it binds before roughly half the subscription channels reach their
individual mark. A ceiling at or above 4 MiB would be dead policy, because
per-channel pressure would always trip first.

#### 9.2 One source of truth for the aggregate

`aggregate_buffered` is a **derived function, not a stored counter**:

```
aggregate_buffered() = Σ channel.bufferedAmount
                       over every Bound subscription channel on this peer
```

It is read from the transport at each decision point. There is no running total,
no increment on send, and no decrement on `bufferedAmountLow`.

The previous revision maintained a counter incremented per accepted send and
decremented on `bufferedAmountLow`. That is not implementable: the low-water
event carries no byte delta, so the decrement has no value to use, and bytes the
transport drains between events are never subtracted, so the counter drifts
upward until it wedges the peer. Summing live values has neither problem, because
it never tries to track drains at all — it reads current depth.

The cost is one summation over at most 32 integers per decision, which is
negligible beside the AES-GCM pass and the transport write it guards.

`AGGREGATE_BUFFERED_LOW` is the resume threshold only: a class held by aggregate
pressure resumes when `aggregate_buffered()` falls to or below it. It is not a
counter target.

#### 9.3 Enforcement at every transition

| Transition | Check | Action when the limit is reached |
|------------|-------|----------------------------------|
| Admission (`Reserved`) | subscription channel count, and `aggregate_buffered() ≥ AGGREGATE_BUFFERED_HIGH` | Reject the admission with a typed operator error **on the control channel**, which §9.1 guarantees can always send. Create no channel. Close nothing that already exists. |
| Channel `open` → `Bound` | route still `Reserved`, generation not suppressed | Close the channel, release the reserved slot (§8.2 step 6). |
| Accepted send on a subscription channel | that channel's `bufferedAmount`, then `aggregate_buffered()` | Apply that channel's class rule from the per-channel table. Terminal reports pressure to Core; entity closes its own channel; event records a gap. Hub never drops a terminal frame and never closes a sibling channel. |
| Accepted send on the control channel | control frame count and control queue bytes only | The existing typed overflow response. The aggregate is not consulted. |
| `bufferedAmountLow` on any channel | re-read `aggregate_buffered()` and that channel's `bufferedAmount` | Resume a held class when both are at or below their low marks. No byte delta is needed or used. |
| Route `Retired` | release the slot; the channel leaves the sum by closing | None. |

The two predicates are exact, and they are **not** the same comparison. Admission
rejects on the current value; a send refuses on the value plus the frame it is
about to write:

```
reject admission when aggregate_buffered()             >= AGGREGATE_BUFFERED_HIGH
refuse send      when aggregate_buffered() + frame_len >  AGGREGATE_BUFFERED_HIGH
```

The gap matters. An aggregate strictly below the ceiling arms the send refusal
while leaving admission open, so the two conditions are reachable independently.
§14.3 lands the aggregate exactly on the ceiling to arm both at once.

The send is refused **before** the write, so the ceiling is never exceeded rather
than detected afterwards. A consequence worth stating, because it constrains how
this can be tested: no test may drive production traffic *past* the ceiling. The
observable event is the refusal at the boundary. §14.3 gives the exact reachable
setup.

Per channel:

| Class | Queue frames | Queue bytes | `bufferedAmount` low | `bufferedAmount` high | Overflow behavior |
|-------|--------------|-------------|----------------------|-----------------------|-------------------|
| Control | 16 pending requests (current `LOCAL_WEBRTC_PENDING_REQUESTS`) | 1 MiB = 1,048,576 B total queued, and 64 KiB = 65,536 B maximum per frame (current `LOCAL_WEBRTC_MAX_FRAME_BYTES`) | 64 KiB | 128 KiB | Current `queued_request_overflow_response` on the frame count. On the new byte total, the same typed overflow response. Never close the control channel for queue pressure. |
| Terminal (`term`) | 1 (the Core adapter's single active-write slot) | none — Hub keeps no terminal queue | 64 KiB | 128 KiB | Report `TerminalAdapterPressure::Full` or `WouldBlock` to Core. Hub never queues, never drops, never retries. Core owns the policy. |
| Entity (`ent`) | 64 (current `ENTITY_SUBSCRIPTION_QUEUE_CAPACITY`) | 1 MiB | 64 KiB | 128 KiB | Close that subscription channel with a typed `entity_subscription_overflow` reason on the control channel. Entity state is snapshot-recoverable by re-subscribe, so closing loses no durable state. |
| Package event (`evt`) | `package_event_plane.consumer_queue_max_events` (default 128) | `consumer_queue_max_bytes` (default 2 MiB) | 64 KiB | 128 KiB | Existing Hub `event_gap` policy. Do not close the channel. |

Notes:

- The `bufferedAmount` values keep the current per-channel thresholds
  (`LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW` = `LOCAL_WEBRTC_MAX_FRAME_BYTES` = 64 KiB;
  `_HIGH` = 2 × that = 128 KiB). Only their scope changes, from per-peer to
  per-channel.
- The aggregate ceiling and its enforcement points are defined in the
  per-connection table above. It is 2 MiB, chosen to bind before the per-channel
  marks, and it is checked on every accepted send.
- Every existing Hub-owned event bound — count, byte, payload, fanout, rate,
  queue age — stays Hub-owned and unchanged
  (`src/config.rs:345-361`). `ticket_1787600682_233928` registers the `evt` class
  in this one table and adds no second limit path.

## 10. Ownership contract

- **Core** is the terminal duplex and subscription authority. Core owns attach,
  snapshots, input bytes, output bytes, mode-gated input, resize, ordering,
  bounded queues, pressure, generation, close, recovery, and teardown. Core
  publishes `@trybotster/terminal-protocol` with the required
  `transport=duplex_binary` feature token.
- **Hub** is the content-blind terminal admission and adapter host. Hub admits a
  subscription, creates its channel, binds a content-blind adapter, enforces the
  §9 limits, and observes closure. Hub does not decode, translate, relay,
  acknowledge, retry, or schedule terminal bytes.
- **Entity and package-event channels** are Hub-authored, Hub-bounded, and
  isolated per subscription. Hub authors those frames, so Hub is not blind to
  them; content-blindness applies to terminal channels only. Hub validates
  admission and ownership and adds no cross-channel delivery ordering.
- **Lua plugins** are the composition layer. Lua stays outside terminal attach,
  input, and output paths, as proved in §6.

## 11. Runtime-teardown lens answers

### 11.1 `teardown_class_applies`

Yes. The project changes WebRTC peer and DataChannel lifecycle, per-subscription
generation, close and recovery state, and multi-peer ownership sweeps.

### 11.2 `teardown_isolation`

- One failed **subscription channel** kills exactly: its `WebRtcMuxRoute` entry
  keyed `(session_id, subscription_id, generation)`, its bound Core adapter, its
  queue, and its pressure state. Every sibling channel on the same peer keeps
  working. Every sibling peer keeps working.
- One failed **peer** kills: the control channel, every subscription channel on
  that peer, every `WebRtcMuxRoute` for that `grant_id`, every entity holder and
  connection-scoped event holder for that grant, and the grant itself. Sibling
  peers keep working.
- The one documented exception is ultimate close failure, §11.6.

Isolation is preferred over a shared resource here precisely because the current
shared channel forces healthy-sibling sacrifice: §7.1 shows a ready entity frame
defers terminal output for an unrelated subscription on the same peer.

### 11.3 `teardown_bounds`

- `LOCAL_WEBRTC_PEER_CLOSE_BOUND` (3 s production, 200 ms test,
  `src/local_webrtc.rs:58-62`) already bounds `peer.close()`. The same bound
  applies to each `DataChannel::local_close()`.
- A channel close that exceeds the bound must not block peer cleanup. Per
  `[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`,
  Hub marks the channel closed, retires the route, and continues cleanup.
- The hard stop that ends every driver loop is the existing
  `peer_terminal_rx` watch. Each subscription channel loop selects on it, so a
  peer-terminal cause ends the loop without waiting on channel I/O.
- No unbounded `block_on(close)` may appear on the Hub control plane.
- `[[a ready WebRTC send must win over a queued DataChannel close]]` is preserved:
  an accepted in-flight send resolves before a queued close on the same channel.

### 11.4 `late_message_matrix`

| Message | Owner tag | Reject after terminal failure | Sweep on race with `PeerClosed` |
|---------|-----------|-------------------------------|----------------------------------|
| `Hello` (control channel) | `grant_id` | `cleanup_sent` already gates `RegisterWebrtcAdmission` (`src/local_webrtc.rs:1360`) | Admission never registered |
| `Attach` | `(grant_id, session_id, subscription_id, generation)` | Reject with a typed operator error when the peer is dying or the grant is gone | `detach_local_webrtc_subscriptions` plus `mux.suppress_generation` (`src/daemon_transport.rs:3163`, `:3820-3830`) |
| `Detach` | same triple | Idempotent; an unknown triple is a no-op, not an error | none needed |
| `SubscribeEntities` | `(grant_id, subscription_id, generation)` | Reject after terminal cause; create no channel | Grant-scoped holder release on `PeerClosed` |
| `UnsubscribeEntities` | same | Idempotent | none needed |
| `SubscribePackageEvents` | `(grant_id, owner, event name, generation)` | Reject after terminal cause; create no channel | Connection-scoped holder release per `[[Client event holders are connection-scoped]]`, with admitted-job survival per `[[admitted event holders survive producer unload until Core completion]]` |
| **`DataChannel` `open` for a Hub-created subscription channel (new surface)** | the §8.3 label triple, held as a `Reserved` route | §8.2 step 5 re-checks route state, generation suppression, and peer liveness at `open`. Any failure closes the channel and **binds no Core adapter**. This is reachable only because §8.2 defers the bind out of admission. | The existing `suppress_generations` set is the sweep. A late `open` finds it and self-closes, releasing the `Reserved` slot. |
| **`Reserved` route that never opens (new surface)** | the same triple | `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` expires (§8.2 step 7) | Hub closes the channel, releases the slot, and emits `subscription_channel_open_timeout`. Without this the slot would leak against the §9 channel count. |
| Any peer-originated `DaemonRequest` on the control channel | `grant_id` | `local_webrtc_peer_gone_request_error` (`src/daemon_transport.rs:6025`) | none needed |

The last-but-one row is the genuinely new ownership surface this project adds. A
Hub-created channel can open after the subscription it names was already retired.
Binding a Core adapter at that point would resurrect a dead route.

### 11.5 `production_path_proof`

Bind path, in the two phases of §8.2. The split matters here: the Core adapter is
created in phase two, so a retirement that lands between the phases has a real
guard to hit.

Phase one, admission (§8.2 steps 1–4):
browser control-channel `Attach`
→ `run_data_channel` (`src/local_webrtc.rs:1208`)
→ `ControlMessage` to the owner thread
→ `handle_control_request` (`src/daemon_transport.rs:3196`)
→ `WebrtcTerminalAdmission::Admitted`
→ `WebRtcConnectionMux` route insert as `Reserved`, charged against §9
→ Hub creates the labeled channel
→ control response carrying the label. **No Core adapter yet.**

Phase two, channel `open` (§8.2 step 5):
DataChannel `open`
→ re-check route is `Reserved`, generation not in `suppress_generations`, peer not
  dying
→ Core `bind_terminal_adapter`
→ route becomes `Bound`.

Failure arms of phase two: any failed check, or no `open` within
`LOCAL_WEBRTC_CHANNEL_OPEN_BOUND`, closes the channel under the §11.3 bound and
releases the `Reserved` slot. Neither arm calls `bind_terminal_adapter`, so
neither needs a Core detach.

Teardown path:
`LocalWebrtcHandler::on_connection_state_change` (`src/local_webrtc.rs:1083`)
→ `observe_peer_connection_state`
→ `cleanup_once(cause)`
→ bounded `local_close()` per subscription channel, then per peer
→ route retire
→ `ControlMessage::PeerClosed`
→ `detach_local_webrtc_subscriptions` (`src/daemon_transport.rs:3163`)
→ Core detach
→ adapter `Drop`.

Live oracles required at Verify, not terminal JSON records alone:

1. The dedicated-runtime worker count returns to its pre-connection baseline
   (thread join), the oracle already used for peer teardown.
2. Core terminal inventory reports zero routes for the retired `grant_id`.
3. The public occupancy union — Hub routes plus Core inventory, per
   `[[a public occupancy oracle must union Hub routes with Core inventory]]` — is
   empty for that grant.
4. Red-on-revert control: move `bind_terminal_adapter` back into admission
   (§8.2 step 3, the order the first plan specified) and assert that oracle 2
   leaks a route and that this becomes the **first** failure, per
   `[[a regression test must be shown to go red with the fix reverted]]`.
5. `Reserved` slot accounting returns to its pre-request value after every
   failure arm, so a retired or timed-out route cannot consume §9 budget.

`[[terminal webrtc failure records do not prove peer runtime teardown]]` applies:
`local-webrtc-sender-terminal.json` is not accepted as teardown proof.

### 11.6 `ownership_identity`

- Terminal rows: `(session_id, subscription_id, generation)` — already the
  `WebRtcMuxRoute` key.
- Entity and event rows: `(grant_id, subscription_id, generation)` with a per-grant
  monotonic generation counter.
- Reused-id policy: a delayed `PeerClosed` snapshot removes only rows whose **full
  triple** matches the snapshot. A live peer that reused a subscription id holds a
  strictly higher generation and survives the sweep.
- Owner sweeps must cover both queue orders: closed-first and message-first.

### 11.7 `sibling_fail_closed_policy`

- On successful subscription-channel close: sibling channels on the same peer and
  all sibling peers keep working. Proved by
  `peer_close_leaves_sibling_peers_working` in §15, extended per channel by
  `ticket_1787600674_500120`.
- On successful peer close: sibling peers keep working. Same test.
- On ultimate peer-close failure (the §11.3 bound is exceeded): the documented
  behavior of
  `[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`
  stays unchanged. This project does not widen or narrow that blast radius.
  Proved by `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`
  in §15, which drives the production handler under the close-injection
  environment and asserts both the sacrifice and a complete ownership sweep.
- A `Reserved` route that never reaches `Bound` (§8.2 steps 6 and 7) releases its
  slot and affects no sibling. It holds no Core adapter, so there is nothing to
  detach.

## 12. Extraction map

Project rule: a Hub ticket extracts only what this map assigns to it, moves the
implementation, its policy, and its tests, leaves no forwarding wrapper, and
commits the move separately from the behavior change.

### 12.1 `src/local_webrtc.rs`

| # | Responsibility | Current anchors | Disposition | Target module | Owning ticket |
|---|----------------|-----------------|-------------|---------------|---------------|
| R1 | Signaling, grant issue and TTL, offer/answer, peer state, peer cleanup, `PeerClosed` sweep | `:162`, `:554-636`, `:816-1042`, `:1814` | **Stays** | `src/local_webrtc.rs` | none |
| R2 | Hub control channel loop — Hello, admission, control requests, control responses | `:1208` `run_data_channel`, `:1658` `send_response_frames` | Rewrite in place; loses its entity, event, and terminal arms | `src/local_webrtc.rs` | `ticket_1787600674_500120` |
| R3 | Per-subscription WebRTC channel host — create, label bind, generation, per-channel AES-GCM, binary chunking, per-channel `bufferedAmount` pressure, close, recovery, §9 limit table | new; replaces the shared-channel logic at `:1999-2196` | **Extract** | `src/webrtc_subscription_channel.rs` (new) | `ticket_1787600674_500120` |
| R4 | Terminal egress onto the shared channel | `:2140` `framed_daemon_terminal_frame`, `:2158` `flush_webrtc_adapter_frames` | **Move** into R3 | `src/webrtc_subscription_channel.rs` | `ticket_1787600674_500120` |
| R5 | Terminal ingress over WebRTC | none today | **New**, lands in R3 | `src/webrtc_subscription_channel.rs` | `ticket_1787600674_500120` |
| R6 | Entity frame transport binding on the shared channel | `:1899` `encrypt_daemon_entity_frame`, `:2197` `framed_daemon_entity_frame`, `:2281` `entity_frame_subscription_id`, `pending_entity` / `entity_frame_rx` in `:1208` | **Move** to the entity channel host | `src/webrtc_subscription_channel.rs` | `ticket_1787600682_233928` |
| R7 | Host event transport binding on the shared channel | `:1960` `framed_daemon_event`, `:1980` `host_event_ready`, `:1988` `take_host_event`, `:2107` `flush_webrtc_host_events` | **Move** to the event channel host | `src/webrtc_subscription_channel.rs` | `ticket_1787600682_233928` |
| R8 | Cross-lane fair write over the shared channel | `:1999` `flush_ready_webrtc_host_control`, `:2007-2073` | **Delete** the `Entity` and `Event` arms; the remainder collapses into a plain control writer | `src/local_webrtc.rs` | `ticket_1787600682_233928` |

Entity and package-event **frame authoring** stays where it is —
`src/daemon_entity_subscriptions.rs`, `src/package_entity_fanout.rs`, and
`src/package_event_router.rs`. Only the transport binding moves. This preserves
"Hub-authored, Hub-bounded" from §10.

### 12.2 `src/daemon_transport.rs`

| # | Responsibility | Current anchors | Disposition | Target module | Owning ticket |
|---|----------------|-----------------|-------------|---------------|---------------|
| D1 | Unix accept, framing, handshake, connection cleanup | `:487`, `:572`, `:1957-2185` | **Stays** | `src/daemon_transport.rs` | none |
| D2 | Owner loop, maintenance, pump, close-event, reconcile phases | `:166-260`, `:5478-5648` | **Stays** | `src/daemon_transport.rs` | none |
| D3 | Host control request dispatch | `:3196`, `:3515` | **Stays** | `src/daemon_transport.rs` | none |
| D4 | Terminal JSON handlers — `SendInput`, `ModeGatedInput`, `Resize` | `:2732-2734`, `:3834`, `:3849`, `:3874`, `:6011-6014`, `:6326` | **Delete** after every consumer moves | removed | `ticket_1787600679_990088` |
| D5 | Unix mux writer — terminal, entity, event, and host frame interleaving | `:812` `MuxWriteState`, `:861` `PendingMuxClass`, `:867` `PendingMuxFrame`, `:878`, `:883`, `:906`, `:941` `flush_unix_mux_writes`, `:1040`, `:1060`, `:1071`, `:1079`, `:1113` | **Extract** | `src/unix_subscription_channel.rs` (new) | `ticket_1787603671_590198` |
| D6 | Unix entity subscription stream | `:1864` `handle_entity_subscription_async` | **Stays** — out of project scope | `src/daemon_transport.rs` | none |
| D7 | Fair-write call sites | `:815`, `:947-1005`, `:1552` | **Delete** with the file | removed | `ticket_1787600682_233928` |
| D8 | Admission records — `UnixTerminalAdmission`, `WebrtcTerminalAdmission`, `PendingRuntimeState` | `:5403`, `:5417`, `:5435` | **Stays**; gains §9 channel accounting | `src/daemon_transport.rs` | `ticket_1787600674_500120` |
| D9 | Attach occupancy, `record_attached_subscription_change`, `detach_local_webrtc_subscriptions`, generation suppression | `:2403`, `:3163`, `:5793-5895` | **Stays** | `src/daemon_transport.rs` | none |

### 12.3 `src/host_control_fair_write.rs`

Whole file: **delete** once only `HostControlClass::Control` remains.
Owning ticket: `ticket_1787600682_233928`. No "record why a single-class scheduler
must remain" escape.

### 12.4 Files with no extraction assignment

`src/runtime.rs`, `src/packages.rs`, `src/main.rs`, `src/daemon_maintenance.rs`,
`src/package_event_router.rs`, `src/daemon_entity_subscriptions.rs`,
`src/webrtc_terminal_adapter.rs`, `src/unix_terminal_adapter.rs`. This project
does not migrate their responsibilities. `webrtc_terminal_adapter.rs` and
`unix_terminal_adapter.rs` gain ingress support in place; that is a contract
extension, not an extraction.

### 12.5 Same-repository concurrency

**The registered graph already serializes the two Hub transport tickets, and this
plan preserves that order.** `dependency_1787604800_782527` records
`ticket_1787603671_590198` depends_on `ticket_1787600674_500120`, created at
1787604800, before this plan was written. `ticket_1787603671_590198` also depends
on `ticket_1787600672_342292` through `dependency_1787603766_484335`.

The first plan asserted that this edge "is not required". That was wrong. It came
from checking `project_pipelines_list_ticket_dependencies` for this ticket only,
which returns empty, and not for the sibling tickets. The registered graph is
authoritative; a plan does not get to retire an edge it did not read.

Resulting Hub order: `ticket_1787600674_500120` merges first, then
`ticket_1787603671_590198`.

What `ticket_1787603671_590198` consumes from `ticket_1787600674_500120`:

- The merged Hub commit on `main` that contains `src/webrtc_subscription_channel.rs`.
  These are workspace-internal Rust modules in one repository, so merge to
  `origin/main` is sufficient availability proof. No package artifact is involved,
  which is the source-coupled case in
  `[[closed dependency tickets signal merged source not a consumable release]]`.
- The single §9 limit table and its accounting site, implemented once by
  `ticket_1787600674_500120`. `ticket_1787603671_590198` registers the Unix classes
  in that table and adds no second limit path.
- The route state machine `Reserved` → `Bound` → `Retired` from §8.2, which the
  Unix adapter reuses without a second ordering model.

`ticket_1787603671_590198` must record the exact `ticket_1787600674_500120` merge
commit it built against in its own gate evidence, and must re-verify that base
independently rather than trusting this plan's text.

The two tickets still own disjoint new modules
(`webrtc_subscription_channel.rs` versus `unix_subscription_channel.rs`), which
limits semantic-rebase conflict once the order above is respected. Each ticket
still checks active sibling tickets before planning.

No open ticket outside this project targets `botster-hub`; the open-ticket scan
across all projects returned 32 open tickets, 12 of them in this project and none
of the other 20 on `tgt_7e208a0c76a44980a83b63af976b1f22`.

## 13. Symbol classification

**Rewrite** (the symbol survives with changed behavior):

- `run_data_channel`, `LocalWebrtcPeerState`, `claim_data_channel`,
  `LocalWebrtcFlowControl`, `send_text_or_peer_terminal`,
  `apply_data_channel_event`, `LocalWebrtcHandler::on_data_channel`.
- `WebRtcConnectionMux`, `WebRtcMuxRoute`, `WebRtcTerminalAdapter`,
  `WebRtcTerminalAdapterHandle` — gain ingress and per-channel binding.
- `UnixConnectionMux`, `UnixTerminalAdapter`, `UnixTerminalAdapterHandle` — same.
- `WebrtcTerminalAdmission`, `UnixTerminalAdmission`, `PendingRuntimeState` —
  gain §9 channel accounting.
- `secret_stream_key` — gains per-channel derivation.

**Delete**:

- `DaemonRequest::SendInput`, `DaemonRequest::ModeGatedInput`,
  `DaemonRequest::Resize` and their handlers, response kinds, operation labels,
  and `daemon_mode_gated_input`.
- `HostControlClass`, `MAX_HOST_FRAMES_PER_FLUSH_TURN`,
  `next_ready_host_control_class`, and the whole
  `src/host_control_fair_write.rs` file.
- `flush_ready_webrtc_host_control`'s `Entity` and `Event` arms;
  `flush_webrtc_host_events`.
- `LOCAL_WEBRTC_PENDING_REQUESTS` pressure on terminal input — the deque stays for
  control only.
- The Web `terminalInputQueue` path (owned by `ticket_1787600676_914408`).

**Unchanged**:

- Signaling, grant issue and TTL, offer/answer, bootstrap origin binding.
- `ShutdownSession` classification and generation suppression.
- Attach occupancy, `ready_then_history`, incremental attach phases.
- Every package, plugin, MCP, persistence, supervision, capability, and update
  surface.
- Ghostty terminal semantics.
- All Hub-owned event bounds in `PackageEventPlaneOptions`.
- `handle_entity_subscription_async` (Unix entity stream).

## 14. Cross-repository acceptance matrix

Deterministic invariants gate every row. Wall-clock values are recorded only as
reference-runner evidence on `botster-ubuntu-24.04-16core`.

| # | Invariant | Deterministic gate | Repository | Owning ticket |
|---|-----------|--------------------|------------|---------------|
| A1 | `TerminalAdapter` carries ingress and egress | Core conformance harness passes duplex arms | botster-core | 672 |
| A2 | Core rejects stale-mode input deterministically | Unit: stale `(mode_generation, mode_revision)` returns the typed gated outcome | botster-core | 672 |
| A3 | `transport=duplex_binary` is required in Hello | Wrong-token ablation fails compatibility with the typed diagnostic | botster-core, botster-hub | 672, 674 |
| A4 | One peer, one control channel, N subscription channels | Hub test asserts channel count and labels for 3 concurrent terminals | botster-hub | 674 |
| A5 | Terminal output shares no queue with control, entity, or event | Hub test: saturate the entity channel, assert terminal frames still leave in order | botster-hub | 674, 682 |
| A6 | A slow subscription does not block a sibling | Hub test: hold one channel at `bufferedAmount` high, assert a sibling terminal delivers | botster-hub | 674 |
| A7 | §9 limits are enforced from one table | Hub test: 33rd subscription is rejected with the typed error; no channel is created | botster-hub | 674 |
| A8 | A late channel `open` on a suppressed generation binds no adapter | Drive the production failure handler to retire the route, then fire `open`; assert Core terminal inventory holds zero routes for the grant. Red-on-revert: move the bind back into admission (§8.2 step 3) and assert this becomes the first failure. | botster-hub | 674 |
| A8b | A `Reserved` route that never opens releases its slot | Withhold `open` past `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND`; assert the typed timeout event and that the §9 channel count returns to its pre-request value | botster-hub | 674 |
| A8c | Both admission race orders are covered | Run A8 in both queue orders, retire-then-open and open-then-retire | botster-hub | 674 |
| A9 | `host_control_fair_write.rs` is gone | `grep` returns zero matches; build succeeds | botster-hub | 682 |
| A10 | Entity and event channels are isolated and Hub-bounded | Hub saturation test: an event flood delays neither terminal nor control | botster-hub | 682 |
| A11 | Unix terminal input uses Core binary frames | Hub test: no `SendInput` JSON on the Unix path; byte order preserved | botster-hub | 671 |
| A12 | Browser terminal input does not wait for a JSON response | Web test: N keystrokes emit N binary frames with zero pending control responses | botster-web | 676 |
| A13 | Attach readiness stays separate from history completion | Web test: input is permitted at READY, before FINISH | botster-web | 676 |
| A14 | TUI terminal input uses Core binary frames | TUI test: stale-mode rejection, typing, paste, resize, reconnect | botster-tui | `ticket_1787603674_865638` |
| A15 | Web entity and event subscriptions use dedicated channels | Web one-document reconnect test; a page reload is not reconnect proof | botster-web | 684 |
| A16 | No runtime reads, writes, parses, serializes, or routes the old terminal path | Integration: `grep` plus a real browser, TUI, Unix, and local Hub run through production engine types | botster-hub | 679 |
| A17 | Byte order and delivery are exact | Integration: byte-fidelity oracle across all four transports | botster-hub | 679 |
| A18 | Hub Rust holds no terminal mechanism ownership | Audit: responsibility map re-checked against §12 | botster-hub | 691 |
| A19 | No Lua runs in terminal hot paths | Audit: repeat the §6 proof | botster-hub | 691 |
| A20 | Post-cut observations compare against the post-Restty transport baseline | Reference-runner evidence only, frozen observation format | botster-web, botster-hub | 689, 679 |
| A21 | Both label-arrival orders resolve (see §14.1) | One-document tests, channel-first and response-first | botster-web | 676, 684 |
| A22 | Reconnect replays pull-owned hydration (see §14.1) | Four-boundary reconnect proof with call-site ablation | botster-web | 676, 684 |
| A23 | The live Hub carries the new contract (see §14.2) | Fresh-target locked build, distinct Hub and Core provenance, Hello feature advertisement | botster-hub | 674, 679 |
| A24 | Consumers contain the contract they claim (see §14.2) | Cargo rev proof and installed-npm token proof | botster-web, botster-tui | 672, 676, 673 |
| A25 | At the exact aggregate ceiling, both a rejected admission and a refused send report through control | Exact setup in §14.3, **strictly serial**. Admit 31 channels while the aggregate is 0, fill to exactly 2,097,152 B, attempt the 32nd subscription **first**, then the 65,536 B send on `C_cross`. Assert the aggregate-driven rejection with a free slot, refusal before the write, a **handler-boundary order trace** showing E2 before E3 and E4 with the aggregate still at 2,097,152 B, **separately** end-to-end client receipt of the typed reason with no ordering requirement, no sibling closed, and recovery to 1,998,848 B. Red-on-revert: return control to the aggregate budget and assert assertion 5 fails first. | botster-hub | 674, 682 |
| A26 | The aggregate does not drift | Saturate, drain fully, and assert `aggregate_buffered()` returns to 0 and held classes resume. A stored counter that misses transport drains fails this; the derived sum of §9.2 passes it. | botster-hub | 674 |
| A27 | A refused terminal send is backpressure, not loss | §14.3 A27 sequence. Substitute a terminal channel into the exact-ceiling setup, then attempt a 65,536 B terminal send. Assert `try_write` returns `WouldBlock`, `pressure()` is `WouldBlock`, Core retains the frame while the adapter does not, the aggregate stays at 2,097,152 B, and after draining below 1,048,576 B the same frame is delivered byte-exact with no duplicate. Drive at most 8 attempts before the drain, far below Core's `WRITE_ATTEMPT_BUDGET` of 512. | botster-hub | 674 |
| A27b | Sustained aggregate pressure hits Core's documented hard stop | Hold pressure through 512 consecutive unsuccessful attempts without draining. Assert Core `hard_stop`s the route, emits `ClientWorkerTeardown`, and Hub retires the route. Proves the real end state instead of claiming unbounded retention. | botster-hub | 674 |

### 14.1 Browser request-race and SPA request-state proof

§8.2 makes the browser correlate an `ondatachannel` event with a control response
by label. Those two arrive on different transports, so both orders occur and both
must be tested. `[[spa-patterns]]` and
`[[botster browser pull requests must retry after webrtc reconnect]]` make this a
required product proof, not an edge case.

Required one-document tests, owned by `ticket_1787600676_914408` for terminal and
`ticket_1787600684_892051` for entity and event:

| # | Case | Assertion |
|---|------|-----------|
| 1 | Channel-first: `ondatachannel` fires before the control response | The client parks the channel by label and binds it when the response arrives. It does not discard the channel. |
| 2 | Response-first: the control response arrives before `ondatachannel` | The client parks the pending request by label and binds when the channel opens. |
| 3 | Open timeout | `subscription_channel_open_timeout` (§8.2 step 7) clears the pending request and surfaces a typed state. No orphan pending entry remains. |
| 4 | Cancellation | The user closes the view between request and channel open. The client unsubscribes, and Hub retires the `Reserved` route. |
| 5 | Stale generation | A channel whose label generation is older than the live subscription is closed by the client and never bound. |
| 6 | Pending-request cleanup | After every case above, the pending-request map is empty. A leak here is the SPA request-state defect this row exists to catch. |

Reconnect proof must follow the four boundaries of
`[[a page reload is not a reconnect]]`, because a reload reruns first-connect
hydration and can stay green with the reconnect listener deleted:

1. Stamp a sentinel on `globalThis` before the probe and assert it survives, so
   document replacement is observable.
2. Close the real `RTCDataChannel` in place through a harness-only seam, observe
   `closed`, and wait for the client to reopen without navigation.
3. Count a new request and a new authoritative projection in the new peer
   generation against a pre-probe baseline. Retained values are not sufficient.
4. Remove the production reconnect call site and prove the new assertion becomes
   the **first** failure, per
   `[[a regression test must be shown to go red with the fix reverted]]`.

Per-subscription channels raise the stakes: after reconnect the client must
re-request every pull-owned family **and** rebind every subscription channel by
new label and generation. A reconnect that restores the control channel alone
leaves every terminal, entity, and event view dark.

### 14.2 Live Hub pin and consumed-artifact proof

The first plan asserted the contract without proving that any consumer contained
it. `[[closed dependency tickets signal merged source not a consumable release]]`
makes merge insufficient for artifact-coupled consumers, and
`[[hub generated protocol changes are a four site release chain]]` names the four
states that must converge.

Coupling per consumer:

| Consumer | Coupling | Sufficient availability proof |
|----------|----------|-------------------------------|
| botster-hub → botster-core | Cargo Git rev in `Cargo.toml` and `Cargo.lock` | The exact merged Core rev is resolvable and the locked build succeeds. |
| botster-tui → botster-hub-client, botster-terminal-protocol | Cargo Git rev | Same, against the merged Hub and Core revs. |
| botster-web → `@trybotster/terminal-protocol`, `@trybotster/hub-test-support` | **npm artifact** | Merge is necessary and not sufficient. The published coordinate must be inspected for the contract bytes. |

Required Hub live proof, from `[[live hub proof records distinct hub and locked core binary provenance]]`
and the charter's fresh-target rule:

1. Start from a fresh checkout at the exact merged Hub SHA.
2. `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
3. `cargo build --locked -p botster-hub`.
4. Read the Core rev from that checkout's `Cargo.lock`; it is a **distinct**
   identity from the Hub SHA and must be recorded separately. Filesystem
   colocation under one target directory is not shared provenance.
5. Resolve both executable realpaths and require them under the fresh checkout's
   target directory.
6. Record the Hub SHA, the locked Core SHA, both build commands, and both
   resolved paths in the verification artifact.

Required contract-identity proof:

- **Hello, not DTO grep.** Per
  `[[WebRTC adapter admission uses a Hello feature string not a generated DTO token]]`,
  `transport=duplex_binary` must be proved through the Hello `required_features`
  exchange and the live Hub feature advertisement. A scan of
  `daemon-protocol.ts` for the token is not proof and can falsely fail, because
  the generated TypeScript need not contain the feature literal.
- **Installed content, not coordinates.** For botster-web, install the pinned
  `@trybotster/terminal-protocol` and `@trybotster/hub-test-support` coordinates
  into a clean consumer and grep the installed tree for the contract tokens,
  resolving through exported roots rather than `package.json`, per
  `[[clean consumer smokes resolve exported root entrypoints not package json]]`.
- **Lock sources.** `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  must pass on the Hub branch before any downstream consumption ticket starts.
- **Wrong-rev ablation.** A Cargo identity claim needs a matching-rev green arm
  and a wrong-rev arm that fails to compile, per
  `[[a Cargo source identity proof needs a wrong tag ablation]]`.

If a required npm coordinate does not contain the tokens, the owner opens and
registers a release ticket on the upstream target and blocks the downstream run.
Do not hand-author generated DTOs, and do not point default tests at a sibling
checkout.

### 14.3 Exact aggregate-ceiling setup for A25, A26, and A27

§9.3 refuses a subscription send **before** the write whenever
`aggregate_buffered() + frame_len > AGGREGATE_BUFFERED_HIGH`. The ceiling is
therefore never exceeded, and no test may ask production traffic to exceed it.
The observable event is the **refusal at the boundary**, not a breach.

Refusal predicate, stated once so the test and the sender agree:

```
refuse when aggregate_buffered() + frame_len > AGGREGATE_BUFFERED_HIGH
            (2,097,152 B)
```

Two predicates are in play and they differ. The send refusal is strict-greater on
`+ frame_len`; the admission rejection is greater-or-equal on the current value:

```
refuse send    when aggregate_buffered() + frame_len >  AGGREGATE_BUFFERED_HIGH
reject admit   when aggregate_buffered()             >= AGGREGATE_BUFFERED_HIGH
```

The setup must therefore land the aggregate **exactly at** 2,097,152 B, not below
it. A setup that stops short arms the send refusal but leaves admission open, so a
concurrent subscribe would be admitted and any assertion demanding its rejection
is unsatisfiable.

Setup, all through the production sender:

| Step | Value | Why |
|------|-------|-----|
| 1. Admit entity channels | **31**, while `aggregate_buffered()` is 0 | Every admission must pass the aggregate predicate, so all admissions happen before the fill. 31 is under `MAX_SUBSCRIPTION_CHANNELS` = 32, which leaves one free slot so the later rejection is provably **aggregate-driven and not count-driven**. One of the 31 is the crossing-test channel `C_cross`. |
| 2. Fill 29 channels | 65,536 B each = 1,900,544 B | Each is under the 131,072 B per-channel high-water, so the per-channel rule cannot trip and confound the aggregate assertion |
| 3. Fill 2 channels, one of them `C_cross` | 98,304 B each = 196,608 B | Also under 131,072 B. `C_cross` is given the larger value so its later close produces a distinct, checkable drop |
| 4. Resulting `aggregate_buffered()` | 1,900,544 + 196,608 = **2,097,152 B** | Exactly `AGGREGATE_BUFFERED_HIGH`. Both predicates are now armed: admission is closed, and any further send crosses |
| 5. Attempt a 32nd subscription | — | 31 < 32 so the count limit admits it; only `aggregate_buffered() >= 2,097,152` can reject it. **This runs first**, see below |
| 6. Then attempt one send on `C_cross` | **65,536 B** | 2,097,152 + 65,536 = 2,162,688 > 2,097,152, so the send is refused |

**The two attempts are strictly serial, and the admission runs first.** An earlier
revision ran them concurrently, which is not a deterministic invariant: the
refused send starts the `C_cross` close, and if retirement removed its 98,304 B
before the admission check, `aggregate_buffered()` would be 1,998,848 B and the
subscription would be admitted instead of rejected. The test would pass or fail on
scheduling. Ordering the admission check ahead of the crossing send removes the
race at the source rather than papering over it with a seam or a wait.

#### Production ordering the entity overflow close must follow

The close path is ordered, and this order is a design requirement, not an
implementation detail:

| # | Step |
|---|------|
| E1 | The send refusal is decided, **before** any transport write |
| E2 | Hub **admits** the typed `entity_subscription_overflow` reason to the control channel's send path |
| E3 | Hub calls `local_close()` on that subscription channel under the §11.3 bound |
| E4 | The route moves to `Retired` and its buffered bytes leave `aggregate_buffered()` |

E2 is **admission to the send path, not remote receipt**. A DataChannel send is
asynchronous: Hub can admit the frame at E2, complete E3 and E4, and only then have
the browser receive it. Ordering E2 before E4 therefore proves something about
Hub's own sequencing and nothing about what the browser has seen.

**Close must never wait for remote delivery.** Blocking E3 or E4 on a remote
acknowledgement would put an unbounded network wait on the teardown path, which
§11.3 forbids.

So the two properties are proved separately, by two different oracles:

| Property | Oracle | Ordering requirement |
|----------|--------|----------------------|
| Hub sequences the report ahead of retirement | A production-path order trace inside the handler, recording E2, E3, and E4 in the order they execute | E2 < E3 < E4, asserted on the trace |
| The browser actually receives the typed reason | End-to-end receipt of `entity_subscription_overflow` at the client | **None.** Receipt may land before or after retirement |

E2 must still precede E4 in Hub's own order, for the original reason: a report
admitted after retirement would describe a state that no longer exists, and a
reader correlating it against the aggregate could not distinguish an overflow close
from any other close. The live aggregate assertion is therefore taken **at the
handler boundary**, at the moment E2 is recorded on the trace — not at the moment
the browser reads the frame.

Assertions, in this order:

1. The step 5 subscription receives a typed **aggregate** admission rejection on
   the control channel. Because a free channel slot existed and nothing has closed
   yet, this rejection can only come from the aggregate predicate.
2. `aggregate_buffered()` is still exactly **2,097,152 B** after the rejection.
3. The step 6 send is **refused before the write** (E1).
4. `aggregate_buffered()` is still exactly **2,097,152 B** at the moment of
   refusal. The ceiling was reached and never exceeded.
5. **Handler-boundary order.** The production-path trace records E2 before E3 and
   E4, and `aggregate_buffered()` read at the moment E2 is recorded is still
   exactly **2,097,152 B**. This is an internal assertion about Hub's own
   sequencing; it makes no claim about the browser.
5b. **End-to-end receipt.** The client receives the typed
   `entity_subscription_overflow` reason for `C_cross` on the control channel.
   This assertion carries **no ordering requirement** relative to E3 or E4 —
   receipt may land after retirement, and that is correct behavior. Together with
   assertion 1's rejection also arriving, it proves §9.1 keeps control outside the
   aggregate budget, because both frames crossed a control channel that a saturated
   subscription budget would otherwise have refused.
6. None of the other 30 subscription channels is closed.
7. Recovery: after E4, `aggregate_buffered()` is 2,097,152 − 98,304 =
   **1,998,848 B**, below the ceiling, and a fresh subscribe is admitted again.
   This confirms the assertion 1 rejection tracked the aggregate rather than any
   sticky state.

Red-on-revert arm: return the control channel to the aggregate budget and assert
that assertion 5 fails first, because the saturated budget refuses the very
responses that report the saturation.

A26 reuses this setup, then drains every channel and asserts
`aggregate_buffered()` returns to 0 and held classes resume. A stored counter
that misses transport drains fails at this step; the derived sum of §9.2 passes.

A27 reuses this setup with one **substitution**, not an addition: one of the 29
65,536 B entity channels is replaced by a terminal channel carrying the same
65,536 B. The composition becomes 28 entity channels at 65,536 B, 1 terminal
channel at 65,536 B, and 2 entity channels at 98,304 B. Channel count stays 31 and
the sum stays exactly 2,097,152 B, so the free-slot and ceiling properties of
A25 are preserved. Adding a 32nd channel instead would make the count limit the
binding constraint and destroy A25's proof that the rejection is aggregate-driven.

A27 must then **attempt a terminal send**. §9.3 evaluates pressure on an attempted
send, so aggregate saturation on its own never calls the terminal adapter. An
earlier revision asserted terminal pressure "during the entity refusal", which
exercised nothing: no terminal frame was offered, so no adapter method ran.

A27 sequence, after reaching the exact 2,097,152 B ceiling:

| # | Step | Expected |
|---|------|----------|
| T1 | Core offers one **65,536 B** terminal frame through the production sender | 2,097,152 + 65,536 = 2,162,688 > 2,097,152, so the aggregate predicate refuses it |
| T2 | The adapter returns from `try_write` | `Err(TerminalAdapterWriteError::WouldBlock)` — the single active-write slot is empty, but the transport is not ready. Not `Full`, which means the slot itself is occupied |
| T3 | `pressure()` is polled | `TerminalAdapterPressure::WouldBlock` |
| T4 | Frame ownership | **Core** retains the frame and retries under Core policy, **bounded by `WRITE_ATTEMPT_BUDGET` = 512 consecutive unsuccessful attempts** — see the budget note below; retention is not unbounded. The adapter must not retain it, per the Core contract's "the adapter must not retain the rejected frame". Hub queues nothing |
| T5 | `aggregate_buffered()` after refusal | still exactly **2,097,152 B**; the terminal attempt added no bytes |
| T6 | Drain the entity channels below `AGGREGATE_BUFFERED_LOW` (1,048,576 B) | `pressure()` becomes `Ready` |
| T7 | Core re-offers the same frame | Accepted and delivered **byte-exact**, in order, with no duplicate from the refused attempt |

T4 and T7 together are the real assertion: the refusal is backpressure, not loss.
T7 also proves Hub did not silently retain and replay the frame itself, which
would violate the content-blind adapter contract.

**Core's retention is bounded, and the bound is finite.** At the locked Core
revision `7eafa47`,
`crates/botster-core/src/engine/client_worker.rs:30` sets
`WRITE_ATTEMPT_BUDGET = 512`. Every `WouldBlock` or `Full` result — from either
`try_write` or a `pressure()` poll — increments `unsuccessful_writes`, and at
`>= WRITE_ATTEMPT_BUDGET` Core calls `hard_stop` and tears the route down. A
successful write resets the counter to 0, so the budget counts **consecutive**
failures, not cumulative ones.

"Core retains and retries until the aggregate drains" is therefore only true
below that budget. The plan must not claim unbounded retention.

Two consequences:

1. **Bound the deterministic arm.** T1 through T7 must complete well below 512
   consecutive unsuccessful attempts. The test drives at most **8** attempts
   before the T6 drain, leaving two orders of magnitude of headroom, so a slow
   drain on a loaded runner cannot silently convert this test into a teardown
   test.
2. **Add a long-pressure arm, A27b.** Hold aggregate pressure through 512
   consecutive unsuccessful attempts without draining, and assert the documented
   Core policy actually runs: Core `hard_stop`s the route, emits
   `ClientWorkerTeardown`, and Hub retires the corresponding route. This proves
   the real end state rather than asserting retention that the contract does not
   provide.

Recorded as a product risk, not only a test detail: putting the aggregate ceiling
in the terminal write path means sustained aggregate saturation is converted into
**terminal route teardown** after 512 attempts. That is a behavior change the
implementer must accept deliberately. It is an argument for keeping
`AGGREGATE_BUFFERED_LOW` reachable quickly, and it is why §9.1 excludes control
from the budget — otherwise the teardown notice would itself be unsendable.

A27 also keeps the negative assertion: Hub drops, reorders, or retries no terminal
frame at any point in T1 through T7.

## 15. Hub characterization tests (this ticket)

These tests are added in this ticket's Implement step against `85a0434`. They pin
current behavior so later tickets show an intentional, reviewable change. Each
row states which ticket moves or deletes it.

| Test | Asserts | Later disposition |
|------|---------|-------------------|
| `webrtc_peer_rejects_a_second_data_channel` | `claim_data_channel` is one-shot; the second channel is closed | Rewritten by 674 to "rejects a second **browser-created** channel" |
| `webrtc_shared_channel_carries_control_entity_event_and_terminal_frames` | one channel emits all four classes | Deleted by 674 and 682 |
| `webrtc_ready_entity_frame_defers_terminal_output` | the `src/local_webrtc.rs:1245` gate defers terminal egress | Deleted by 674; replaced by A5 |
| `fair_write_class_coverage_per_transport` | WebRTC rotates `Control`, `Entity`, `Event`; Unix rotates `Control` and `Event` (see §7.4) | Deleted by 682 with the file |
| `terminal_input_travels_as_a_json_control_request` | `SendInput` reaches `HubRuntime::write_bytes` through the control queue | Deleted by 679 |
| `terminal_adapter_contract_is_egress_only_at_the_locked_core_pin` | the pinned Core trait has no ingress method | Deleted by 674 once Core 672 merges |
| `no_lua_dispatch_in_terminal_input_or_output` | §6 invariant | **Kept** through the whole project; re-run by 691 |
| `attach_ready_precedes_history_finish` | `ready_then_history` split | **Kept** |
| `shutdown_suppresses_exact_route_generations_before_core_teardown` | existing suppression order | **Kept** |
| `webrtc_terminal_output_is_byte_exact` | byte fidelity oracle | **Kept**; A17 extends it |
| `peer_close_leaves_sibling_peers_working` | one peer closes successfully; every sibling peer keeps delivering terminal bytes | **Kept**; extended by 674 to sibling *channels* on one peer |
| `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` | the bound in §11.3 is exceeded, the documented dedicated-runtime sacrifice runs through the production handler, and the ownership sweep leaves zero Hub routes and zero Core inventory rows | **Kept**; 674 re-runs it per subscription channel |

§11.7 promised both rows and the first plan shipped neither. They are the only
executable check that the sibling policy is real rather than asserted, so they are
characterization tests now, on current behavior, before any channel work starts.

Both drive the production failure handler
(`on_connection_state_change` → `observe_peer_connection_state` → `cleanup_once`),
not a helper. `ultimate_close_failure_...` uses the existing close-injection seam
and must be run under a daemon child that inherits the injection environment, per
`[[Fault-injected WebRTC close requires a daemon started with the inject env]]`.

Harness: Rust unit and integration tests under `tests/`. No new harness is
introduced.

Commands, in this order:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked -p botster-hub
./test.sh --locked
```

Do **not** use `./test.sh --workspace`. `test.sh` already runs
`cargo test --workspace "$@"`, so the flag arrives twice and Cargo aborts with
`error: the argument '--workspace' cannot be used multiple times` before any test
executes. Verified at this base: the duplicate form exits 1 with that message, and
the session-worker build above completes from Core rev `7eafa470`.

## 16. Assumptions and unknowns

Assumptions:

1. Core `ticket_1787600672_342292` extends `TerminalAdapter` with ingress and
   publishes `transport=duplex_binary`. Verified today: the pinned contract at
   `7eafa47` is egress-only.
2. `botster-web` can handle `RTCPeerConnection.ondatachannel`. Not verifiable from
   this repository. Registered as a precondition for
   `ticket_1787600676_914408`; if it is false, §8.2 must be re-decided before 674
   implements.
3. The `botster-ubuntu-24.04-16core` runner stays available. It is already wired
   at `.github/workflows/loaded-daemon-lifecycle.yml:69`.
4. The §9 numbers are Plan-stage decisions with the rationale stated inline. Plan
   Review may change any of them; downstream tickets must cite §9, not re-derive.
5. `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` is set at 5 s production and 200 ms test.
   The production value is a judgement, not a measurement: no current Hub code
   waits on a Hub-created channel `open`, because Hub creates none today.
   `ticket_1787600674_500120` must measure real open latency on the reference
   runner and raise the bound if 5 s proves tight under renegotiation.

Unknowns, each assigned to an owner rather than resolved here:

1. The exact shape of the Core ingress API — push, pull, or a paired handle. Owned
   by `ticket_1787600672_342292`. This plan states the requirement and refuses to
   invent the Core signature.
2. Whether the browser can create 32 channels without a renegotiation stall.
   Owned by `ticket_1787600674_500120`, measured on the reference runner.
3. Whether per-channel AES-GCM key derivation needs a protocol revision bump.
   Owned by `ticket_1787600674_500120` together with the Hub test-support cutover.

## 17. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Hub-created channels open after the subscription is retired | A resurrected route leaks a Core adapter | §11.4 last-but-one row; A8 with a red-on-revert control |
| Extraction becomes a file-only split | Hub gravity does not fall; `[[Hub extraction must reduce ownership rather than only split files]]` fails at review | §12 assigns responsibilities, not line counts; move-only commits precede behavior commits |
| 33 channels per peer exceed a browser or `webrtc-rs` limit | Admission fails late instead of at the table | A7 tests the boundary; unknown 2 measures it early |
| Deleting D4 too early breaks Web or TUI | Compile break across repositories | D4 is assigned only to `ticket_1787600679_990088`, after 676, 671, and the TUI ticket merge |
| Per-channel AES-GCM adds a copy per terminal frame | Throughput regression on large history | Reference-runner comparison against the post-Restty baseline, A20 |
| Semantic rebase between 674, 671, and 682 | Completed review goes stale | §12.5 disjoint modules; renew review after any semantic rebase |
| Wall-clock assertions flake under suite load | False failures | Deterministic gates only; timing recorded as evidence, never asserted |
| **Sustained aggregate saturation tears down a terminal route.** The §9 ceiling sits in the terminal write path, and Core hard-stops after `WRITE_ATTEMPT_BUDGET` = 512 consecutive `WouldBlock`/`Full` results (`client_worker.rs:30`) | Backpressure silently becomes route teardown under long pressure. This is a real behavior change, not a test artifact | The implementer must accept it deliberately. A27b proves the documented end state; §9.1 excluding control from the budget is what keeps the teardown notice sendable; keeping `AGGREGATE_BUFFERED_LOW` reachable quickly is the mitigation |
| A close-path assertion can confuse Hub enqueue order with remote receipt | A test that looks deterministic passes on send-path timing | §14.3 splits them: a handler-boundary order trace for E2 before E3/E4, and a separate end-to-end receipt assertion with no ordering requirement. Close never waits on remote delivery |

## 18. Vault gaps worth capturing

1. **Hub creates subscription DataChannels after admission.** The creator decision
   in §8.2 and its fail-closed rationale are not in the vault. Capture after
   `ticket_1787600674_500120` proves it.
2. **A subscription channel label binds identity and generation.** The §8.3 scheme
   deserves an atomic note once implemented.
3. **Per-channel AES-GCM binds a frame to its subscription.** The §8.4 derivation
   change and its replay-resistance rationale.
4. **The fair-write scheduler is three-class on WebRTC and two-class on Unix.**
   §7.4 corrects a reading that a prior advisor answer got wrong.
   This is worth capturing now, because it changes the deletion scope of
   `ticket_1787600682_233928`.
5. **Hub content-blindness permits transport framing.** Chunking and encrypting an
   opaque byte string does not violate content-blindness. §8.4 and §8.5 state it;
   the existing note does not.
