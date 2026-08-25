# Isolate control and terminal subscriptions on dedicated DataChannels

Ticket: `ticket_1787600674_500120`
Run: `run_1787664777_379002`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Base commit: `55f620d` on `main` (contains the merged `webrtc 0.21.0-beta.2` roll)

## 1. Target

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Project: `project_1787600579_585482`, Botster Isolated Subscription Data Plane

The target id was resolved through `project_pipelines_get_project`. Every Hub
ticket in this project carries the same target id. The ambient worktree was not
used to choose the repository.

Registered dependencies, both closed:

| Dependency | Ticket | Status |
|---|---|---|
| `dependency_1787600712_947298` | `ticket_1787600672_342292` — Core: make terminal subscriptions duplex and pressure-isolated | closed |
| `dependency_1787654923_752279` | `ticket_1787654915_646236` — Hub: upgrade WebRTC for post-handshake DataChannel creation | closed |
| registered 2026-08-25 | `ticket_1787667162_566252` — Hub: restore the strict Rust gate baseline | open, blocks this ticket |

`ticket_1787667162_566252` was registered during this Plan step, after the strict
Rust gates were measured red on `main`. See section 11.2. Run
`run_1787664777_379002` was cancelled without advancing feature work, and this
ticket restarts from current `main` after the prerequisite merges.

Open sibling tickets on this target: `ticket_1787600682_233928` (entity and
event channels), `ticket_1787603671_590198` (Unix duplex bind),
`ticket_1787600679_990088` (integration cold cut), `ticket_1787600691_401181`
(kernel and Lua boundary audit). Both 682 and 671 depend on this ticket, so this
ticket merges first among the Hub transport tickets.

## 2. Playbooks and notes loaded

Repository playbook: `[[botster-hub-playbook]]`.

Role playbooks, in order:

1. `[[planner-playbook]]`
2. `[[botster-planner-playbook]]`
3. `[[botster-hub-playbook]]`

Class overlay, applied because this ticket is runtime-teardown class:

- `[[botster runtime teardown lenses]]`

Targeted atomic notes:

- `[[botster subscriptions use dedicated ordered DataChannels]]`
- `[[the browser creates each subscription DataChannel after Hub reserves its label]]`
- `[[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]]`
- `[[webrtc 0 21 restores post handshake DataChannel creation in Hub]]`
- `[[rejected channel isolation needs a surviving channel positive control]]`
- `[[core owns duplex terminal transport while Hub stays content blind]]`
- `[[Core terminal subscription ownership is session, subscription, and generation]]`
- `[[WebRTC terminal admission requires an encrypted DataChannel Hello]]`
- `[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`
- `[[a ready WebRTC send must win over a queued DataChannel close]]`
- `[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`
- `[[webrtc peer cleanup removes every per peer owner together]]`
- `[[terminal webrtc failure records do not prove peer runtime teardown]]`
- `[[a public occupancy oracle must union Hub routes with Core inventory]]`
- `[[ShutdownSession suppresses exact route generations before Core teardown]]`
- `[[Hub Core pin rolls update eleven literal sites and six lock sources]]`
- `[[live hub proof records distinct hub and locked core binary provenance]]`
- `[[Hub suite runs prebuild the session worker before the locked test wrapper]]`
- `[[Hub bee15e7 builds the session worker from botster-core-daemon]]`
- `[[Hub test support capability cutovers use a new unpublished package version]]`
- `[[Hub test support version bumps must update the Node mirror test literals]]`
- `[[WebRTC adapter admission uses a Hello feature string not a generated DTO token]]`
- `[[a regression test must be shown to go red with the fix reverted]]`
- `[[Hub extraction must reduce ownership rather than only split files]]`
- `[[frozen repository plan contracts outrank vault convention notes]]`
- `[[express scope limits as invariants not closed enumerations]]`
- `[[Fault-injected WebRTC close requires a daemon started with the inject env]]`
- `[[public protocol versions host control and Core terminal planes independently]]`

`[[project-pipelines-playbook]]` is not loaded. No Project Pipelines package or
plugin path is in scope.

## 3. Context loaded

Repository sources read at base `55f620d`:

- `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md`
  — the frozen architecture contract, sections 8 to 18.
- `src/local_webrtc.rs` (7875 lines) — `on_data_channel` at `:1148`,
  `run_data_channel`, `send_text_or_peer_terminal` at `:1203`, constants at
  `:50-70`, `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`
  at `:6505`.
- `src/webrtc_terminal_adapter.rs` (926 lines) — `WebRtcTerminalAdapter`,
  `WebRtcConnectionMux`, `WebRtcMuxRoute`, `suppress_generations`.
- `src/daemon_transport.rs` (10573 lines) — terminal JSON handlers at
  `:3834-3890`, operation labels at `:6021-6024`, `daemon_mode_gated_input` at
  `:6336`.
- `src/host_control_fair_write.rs` (169 lines) —
  `fair_write_class_coverage_per_transport` at `:132`.
- `crates/botster-hub-client/src/lib.rs` — `PROTOCOL_VERSION = 7`,
  `CONFORMANCE_FIXTURE_REVISION = 46`, `DaemonRequest` at `:1073-1090`.
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` (954 lines) —
  the twelve characterization tests from architecture section 15.
- `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/ci.yml`,
  `.github/workflows/loaded-daemon-lifecycle.yml`.

Upstream source read at `botster-core` `origin/main` = `358ef1a`:

- `crates/botster-core/src/contract/terminal_adapter.rs` — the merged duplex
  `TerminalAdapter` trait now carries `try_read`, `TerminalIngress`, and
  `MIN_ADAPTER_INGRESS_BUFFER_FRAMES = 64`.
- `crates/botster-terminal-protocol/src/lib.rs` —
  `FEATURE_TRANSPORT_DUPLEX_BINARY = "transport=duplex_binary"`.
- `crates/botster-terminal-protocol/src/compatibility.rs` — the default
  requirement now includes `transport=duplex_binary`.
- `crates/botster-core/src/engine/client_worker.rs` — `WRITE_ATTEMPT_BUDGET`.

Answered blocking questions carried forward from the cancelled run
`run_1787653825_278029`:

- `question_1787654317_205897` — option A confirmed. Hub reserves and returns the
  label; the browser creates the subscription channel.
- `question_1787654873_895043` — option 2 confirmed. The WebRTC roll was a
  separate ticket, now merged. This ticket restarts from current `main` and keeps
  the rejected-channel test correction.

Blocking question for this run: `question_1787665047_404406`.

## 4. Scope

In scope:

1. **Roll the Core pin** from `7eafa470a18025895995bbedc20d34b58106a03b` to the
   merged Core revision `358ef1a6bf0f792f6da10d60890be39cb16779d0`, so Hub
   consumes the merged duplex contract. This ticket cannot compile the new
   adapter without it.
2. **Add `src/webrtc_subscription_channel.rs`** — the per-subscription WebRTC
   channel host. It owns reservation, the architecture section 8.3 label scheme,
   route identity, generation, per-channel AES-GCM derivation, binary chunking,
   per-channel `bufferedAmount` pressure, close, recovery, and the single
   section 9 limit table.
3. **Reserve at admission, bind at open.** Admission authorizes, assigns the
   generation, charges the limit table, inserts a `Reserved` route, and returns
   the exact label. Admission creates and binds nothing.
4. **Validate every open event** against route identity, subscription identity,
   generation suppression, peer liveness, and route state before binding the
   content-blind Core adapter.
5. **Reject** late, stale, mismatched, duplicate, unreserved, and over-limit open
   events without binding an adapter.
6. **Retire a `Reserved` route** whose open timeout expires, and emit a typed
   `subscription_channel_open_timeout` host event on the control channel.
7. **Add terminal ingress** to `WebRtcTerminalAdapter` so it satisfies the merged
   `TerminalAdapter::try_read` contract and carries Core binary input frames.
8. **Remove terminal traffic from the shared channel** — move
   `framed_daemon_terminal_frame` and `flush_webrtc_adapter_frames` out of
   `src/local_webrtc.rs` and delete the terminal arm of the shared writer.
9. **Rewrite `claim_data_channel`** to mean "claim the one browser-created
   control channel", and route every other browser-created channel through
   reservation matching.
10. **Correct architecture section 8.2** so the browser creates subscription
    DataChannels after Hub reservation, and keep `Reserved` → `Bound` → `Retired`
    and the section 8.3 label scheme intact.
11. **Daemon protocol DTOs** — carry the reservation label and generation on the
    admission response, add the typed open-timeout host event, bump the Hub
    daemon protocol revision, and cut over `botster-hub-test-support` on a new
    unpublished package version with its Node mirror literals.
12. **Tests** for reservation, connection, replacement, reconnect, stale
    generation, late open, open timeout, pressure, encryption, chunking, and
    limits, plus the section 15 characterization dispositions this ticket owns.

Out of scope:

- Entity and package-event subscription channels, `host_control_fair_write.rs`
  deletion, and the `entity_subscription_overflow` close path. Owned by
  `ticket_1787600682_233928`.
- The Unix duplex adapter bind and `src/unix_subscription_channel.rs`. Owned by
  `ticket_1787603671_590198`.
- Deleting `DaemonRequest::SendInput`, `ModeGatedInput`, and `Resize`. Owned by
  `ticket_1787600679_990088` (architecture section 12.2 row D4).
- Browser and TUI channel creation. Owned by `ticket_1787600676_914408` and
  `ticket_1787603674_865638`.
- Per-subscription SDP renegotiation and any pre-created channel pool. Both are
  refused by the ticket and by the answered question `question_1787654317_205897`.
- Ghostty terminal semantics, signaling, grant issue and TTL, offer and answer,
  bootstrap origin binding, persistence, supervision, packages, plugins, MCP.
- Any responsibility architecture section 12.1 does not assign to this ticket.

Scope limit, stated as an invariant rather than a change count: every changed
line traces to one of the twelve scope items above, to a convention this
repository already enforces, or to cleanup that one of those changes makes
necessary.

## 5. Repository ownership boundaries and cross-repository dependencies

| Boundary | Owner | This ticket |
|---|---|---|
| Terminal attach, snapshots, duplex bytes, mode-gated input, resize, ordering, bounded queues, pressure, recovery, teardown | `botster-core` | consumes the merged contract; adds no second phase machine |
| Terminal payload semantics | `botster-core` | Hub stays content blind; Hub encrypts and chunks an opaque byte string only |
| Admission, reservation, limits, adapter host, signaling, supervision, persistence, plugin isolation | `botster-hub` Rust | owned here |
| Product workflow policy, commands, hooks, lifecycle composition | Lua plugins | unchanged; no Lua runs in the terminal hot path |
| Browser channel creation and binding | `botster-web` | not in this ticket |
| External client DTOs | `crates/botster-hub-client` in this repository | the reservation label and open-timeout event land here |

Cross-repository dependencies:

- `botster-core` `358ef1a` must be resolvable by Cargo Git rev. Hub is
  **source-coupled** to Core through a Cargo Git rev in `Cargo.toml` and
  `Cargo.lock`, so per
  `[[closed dependency tickets signal merged source not a consumable release]]`
  merge to `origin/main` is sufficient availability proof. No Core package
  release exists or is required. The artifact-coupled half of that note applies
  to `botster-web` and the npm packages, which this ticket does not consume.
- No new cross-repository prerequisite is discovered by this plan, so no new
  dependency ticket is registered. If the Core roll turns out to need a Core-side
  fix, that fix is registered against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`
  rather than absorbed here.

## 6. Runtime-teardown lens answers

`teardown_class_applies`: **yes**. This ticket changes WebRTC DataChannel
lifecycle, per-subscription generation, close and recovery state, adapter bind
timing, and the multi-peer ownership sweep.

`teardown_isolation`: one failed subscription channel kills exactly its
`WebRtcMuxRoute` entry keyed `(session_id, subscription_id, generation)`, its
bound Core adapter, its own send state, and its pressure state. Sibling channels
on the same peer and every sibling peer keep working. One failed peer kills the
control channel, every subscription channel on that peer, every route for that
`grant_id`, and the grant. Sibling peers keep working. Isolation is chosen over
the current shared channel precisely because the shared channel forces
healthy-sibling sacrifice: architecture section 7.1 shows a ready entity frame
deferring terminal output for an unrelated subscription.

`teardown_bounds`: `LOCAL_WEBRTC_PEER_CLOSE_BOUND` (3 s production, 200 ms test,
`src/local_webrtc.rs:58-65`) also bounds each `DataChannel::local_close()`. A
channel close that exceeds the bound must not block peer cleanup: Hub marks the
channel closed, retires the route, and continues, per
`[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`.
The hard stop that ends every driver loop stays the existing `peer_terminal_rx`
watch; each subscription channel loop selects on it. No unbounded
`block_on(close)` may appear on the Hub control plane. A ready accepted send
still resolves before a queued close on the same channel, per
`[[a ready WebRTC send must win over a queued DataChannel close]]`.

`late_message_matrix`:

| Message | Owner tag | Reject after terminal failure | Sweep on race with `PeerClosed` |
|---|---|---|---|
| `Hello` on the control channel | `grant_id` | `cleanup_sent` already gates `RegisterWebrtcAdmission` | admission never registered |
| `Attach` | `(grant_id, session_id, subscription_id, generation)` | typed operator error when the peer is dying or the grant is gone; no reservation is inserted and nothing is charged | `detach_local_webrtc_subscriptions` plus `mux.suppress_generation` |
| `Detach` | same triple | idempotent; an unknown triple is a no-op | none needed |
| `SubscribeEntities` / `SubscribePackageEvents` | `(grant_id, subscription_id, generation)` | unchanged in this ticket; still control-channel only | unchanged |
| **Browser-created `DataChannel` open (new surface)** | the section 8.3 label triple, held as a `Reserved` route | the open handler re-checks route state, subscription identity, generation suppression, and peer liveness. Any failure closes the channel and **binds no Core adapter** | `suppress_generations` is the sweep. A late open finds it, self-closes, and releases the reserved slot |
| **`Reserved` route that never opens (new surface)** | the same triple | `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` expires | Hub closes the channel, releases the slot, and emits `subscription_channel_open_timeout`. Without this the slot leaks against the section 9 channel count |
| **Unreserved browser-created channel (new surface)** | none — no route matches its label | closed immediately, fail closed. A label with no `Reserved` route binds nothing and charges nothing | none needed; nothing was created |
| Any peer-originated `DaemonRequest` on the control channel | `grant_id` | `local_webrtc_peer_gone_request_error` | none needed |

The two genuinely new ownership surfaces are the browser-created channel open and
the reservation that never opens. Binding a Core adapter on a channel that opens
after its subscription retired would resurrect a dead route; charging a
reservation that never opens would leak section 9 budget.

`production_path_proof`:

Phase one, admission:
browser control-channel `Attach`
→ `run_data_channel` (`src/local_webrtc.rs:1208`)
→ `ControlMessage` to the owner thread
→ `handle_control_request` (`src/daemon_transport.rs:3196`)
→ `WebrtcTerminalAdmission::Admitted`
→ `WebRtcConnectionMux` route insert as `Reserved`, charged against section 9
→ control response carrying the exact label. **No Core adapter yet, and no
channel created by Hub.**

Phase two, browser-created channel open:
`LocalWebrtcHandler::on_data_channel` (`src/local_webrtc.rs:1148`)
→ label parse
→ match an existing `Reserved` route
→ re-check route state, generation not in `suppress_generations`, peer not dying
→ Core `bind_terminal_adapter`
→ route becomes `Bound`.

Teardown path:
`on_connection_state_change` (`src/local_webrtc.rs:1083`)
→ `observe_peer_connection_state`
→ `cleanup_once(cause)`
→ bounded `local_close()` per subscription channel, then per peer
→ route retire
→ `ControlMessage::PeerClosed`
→ `detach_local_webrtc_subscriptions` (`src/daemon_transport.rs:3163`)
→ Core detach
→ adapter `Drop`.

Live oracles required at Verify, not terminal JSON records:

1. The dedicated-runtime worker count returns to its pre-connection baseline.
2. Core terminal inventory reports zero routes for the retired `grant_id`.
3. The public occupancy union — Hub routes plus Core inventory, per
   `[[a public occupancy oracle must union Hub routes with Core inventory]]` — is
   empty for that grant.
4. Red-on-revert control: move `bind_terminal_adapter` back into admission and
   assert oracle 2 leaks a route and that this becomes the **first** failure, per
   `[[a regression test must be shown to go red with the fix reverted]]`.
5. Reserved-slot accounting returns to its pre-request value after every failure
   arm.

`[[terminal webrtc failure records do not prove peer runtime teardown]]` applies:
`local-webrtc-sender-terminal.json` is not accepted as teardown proof.

`ownership_identity`: terminal rows keep `(session_id, subscription_id,
generation)`, already the `WebRtcMuxRoute` key
(`src/webrtc_terminal_adapter.rs:317-323`) and consistent with
`[[Core terminal subscription ownership is session, subscription, and generation]]`.
A delayed `PeerClosed` snapshot removes only rows whose full triple matches the
snapshot, so a live peer that reused a subscription id holds a strictly higher
generation and survives the sweep. Owner sweeps cover both queue orders,
closed-first and message-first.

`sibling_fail_closed_policy`: on a successful subscription-channel close, sibling
channels on the same peer and every sibling peer keep working. On a successful
peer close, sibling peers keep working. On ultimate peer-close failure the
documented behavior of
`[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`
stays unchanged; this ticket neither widens nor narrows that blast radius. A
`Reserved` route that never reaches `Bound` releases its slot and affects no
sibling, because it holds no Core adapter.

## 7. Affected surfaces and files

New:

- `src/webrtc_subscription_channel.rs` — architecture row R3: reservation, label
  parse and format, route state machine, per-channel key derivation, binary
  chunking and reassembly, per-channel pressure, close, recovery, and the single
  section 9 limit table with its one accounting site. Receives R4
  (`framed_daemon_terminal_frame`, `flush_webrtc_adapter_frames`) as a move, and
  R5 (terminal ingress) as new code.

Changed:

- `src/local_webrtc.rs` — R2: the control channel loop loses its terminal arm.
  `on_data_channel` (`:1148`) stops being one-shot for every channel and becomes
  "claim one control channel, then match reservations".
  `claim_data_channel`, `LocalWebrtcFlowControl`, `send_text_or_peer_terminal`,
  and `apply_data_channel_event` are rewritten. Adds
  `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` beside the constants at `:50-70`.
- `src/webrtc_terminal_adapter.rs` — `WebRtcTerminalAdapter` implements the
  merged duplex trait, including `try_read` and `TerminalIngress`, holding at
  least `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` = 64 complete frames before it may
  report `Lost`. `WebRtcConnectionMux` and `WebRtcMuxRoute` gain the `Reserved` →
  `Bound` → `Retired` state and per-channel binding.
- `src/daemon_transport.rs` — architecture row D8 only: `WebrtcTerminalAdmission`
  and `PendingRuntimeState` gain section 9 channel accounting, and the admission
  response carries the reservation label and generation. Rows D1, D2, D3, D6, and
  D9 stay. Row D4 is not touched.
- `crates/botster-hub-client/src/lib.rs` — reservation label and generation on
  the admission response, the typed `subscription_channel_open_timeout` host
  event, and the Hub daemon protocol revision bump.
- `crates/botster-hub-client/src/typescript.rs` and
  `crates/botster-hub-client/generated/daemon-protocol.ts` — release-chain site 1.
- `crates/botster-hub-test-support/**` and `packages/hub-test-support/**` —
  release-chain site 2: new unpublished package version, regenerated
  `daemon-protocol.ts` and `metadata.json`, and updated `test.mjs` Node mirror
  literals.
- `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-test-support/Cargo.toml`,
  `crates/botster-hub-test-support/build.rs`,
  `crates/botster-hub-test-support/src/lib.rs`,
  `crates/botster-hub-test-support/src/conformance_data.rs`, `Cargo.lock`, and
  the six live-proof test literals — the Core pin roll.
- `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md`
  — the section 8.2 correction.

Tests changed or added:

- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs`
- `src/local_webrtc.rs` unit lanes

Untouched by design: `src/host_control_fair_write.rs`,
`src/daemon_entity_subscriptions.rs`, `src/package_entity_fanout.rs`,
`src/package_event_router.rs`, `src/unix_terminal_adapter.rs`, `src/runtime.rs`,
`src/packages.rs`, `src/main.rs`, `src/daemon_maintenance.rs`.

## 8. The Core pin roll

Target revision: `358ef1a6bf0f792f6da10d60890be39cb16779d0`.

`[[Hub Core pin rolls update eleven literal sites and six lock sources]]` records
eleven literal sites. The count measured at this base is **eighteen**, so the
implementer must use the measured set and not the note's count:

| Site | Count |
|---|---|
| `Cargo.toml` root dependencies and dev-dependencies | 5 |
| `crates/botster-hub-client/Cargo.toml` | 1 |
| `crates/botster-hub-test-support/Cargo.toml` | 3 |
| `crates/botster-hub-test-support/build.rs` (`PROTOCOL_REV`) | 1 |
| `crates/botster-hub-test-support/src/lib.rs` | 1 |
| `crates/botster-hub-test-support/src/conformance_data.rs` | 1 |
| `tests/session_projection_owner_loop.rs` (`REQUIRED_CORE_REV`) | 1 |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | 1 |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | 1 |
| `tests/hub_daemon_lifecycle/package_event_plane.rs` | 1 |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | 1 |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | 1 |

Plus six `source` lines in `Cargo.lock`: `botster-core`, `botster-core-daemon`,
`botster-core-test-support`, `botster-terminal-ghostty`,
`botster-terminal-protocol`, `botster-terminal-protocol-client`.

Every dependency keeps the URL `https://github.com/trybotster/botster-core.git`
and the `rev =` selector. Historical `docs/plans/**` and `docs/reports/**` keep
their original revisions. After the roll, a search for the old SHA outside
`docs/` and `target/` must return zero matches, including in `Cargo.lock`.

The roll also raises the Core terminal-protocol default requirement to include
`transport=duplex_binary`, so
`node packages/hub-test-support/scripts/sync-assets.mjs --check` must pass on the
rolled branch before the test wrapper runs. `test.sh` runs that check first.

## 9. Commit sequence

The project rule requires a move-only commit before the behavior commit, so
review can verify the move separately.

1. **Core pin roll.** Eighteen literal sites plus six lock sources, plus the
   mechanical `TerminalAdapter::try_read` implementation needed to compile
   against the merged trait. No reservation logic.
2. **Move only.** `framed_daemon_terminal_frame` and
   `flush_webrtc_adapter_frames` move from `src/local_webrtc.rs` to
   `src/webrtc_subscription_channel.rs`, together with their tests. No behavior
   change, no forwarding wrapper left behind.
3. **Behavior.** Reservation, label scheme, open-event validation, the section 9
   limit table, per-channel key derivation, binary chunking, per-channel
   pressure, open timeout, and terminal ingress.
4. **Protocol.** DTO fields, revision bump, `botster-hub-test-support` cutover,
   Node mirror literals.
5. **Architecture correction.** Section 8.2 rewritten for browser creation.
6. **Tests.** The acceptance rows in section 11 below.

## 10. Assumptions and unknowns

Two items below were resolved by the human rather than assumed. The owner
recorded the same split on both owning ticket descriptions.

Assumptions, stated explicitly:

1. `botster-core` `358ef1a` is the correct roll target. It is the current
   `origin/main` head and contains the merged duplex work (`065c2bf`,
   `9cf5619`) plus its review returns. Hub consumes Core by Cargo Git rev, so the
   merged `main` head is the consumable artifact; the Implement step re-resolves
   `origin/main` and records the exact SHA it pins rather than trusting this
   text.
2. **Decided, not assumed.** Blocking question `question_1787665047_404406`
   confirmed option 1A. This ticket removes terminal output from the shared
   WebRTC control-channel path and deletes `framed_daemon_terminal_frame`,
   `flush_webrtc_adapter_frames`, and the shared-writer terminal egress branch.
   It adds Core binary ingress on the dedicated terminal channel. It **keeps**
   `DaemonRequest::SendInput`, `ModeGatedInput`, `Resize`, and their
   transport-agnostic handlers. `ticket_1787600679_990088` deletes those handlers
   only after the Web, Unix, and TUI consumers migrate. The Implement and Verify
   reports must **not** describe the retained handlers as completed cold-cut
   work.
3. **Decided, not assumed.** The same answer confirmed option 2A. The exact
   2,097,152 B aggregate setup is built from 31 terminal channels with one free
   slot. Aggregate-driven admission rejection, A26 no-drift, A27 `WouldBlock`
   recovery, and A27b the 512-attempt Core hard stop are all proved here. Only
   the entity-overflow close ordering E1 through E4, assertions 5 and 5b, and the
   section 9.1 red-on-revert control move to `ticket_1787600682_233928`. The
   aggregate implementation and the terminal pressure proof are **not** deferred.
4. `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` starts at 5 s production and 200 ms test.
   The production value is a judgement, not a measurement. This ticket measures
   real open latency on the reference runner and raises the bound if 5 s proves
   tight.
5. The section 9 numbers are frozen architecture decisions. This ticket cites
   section 9 and does not re-derive them.

Unknowns, each with an owner:

1. Whether a browser can create 32 channels without a renegotiation stall. Owned
   by this ticket, measured on `botster-ubuntu-24.04-16core`. Hub's own suite now
   measures the Rust-peer equivalent, because `webrtc 0.21` restores
   post-handshake creation per
   `[[webrtc 0 21 restores post handshake DataChannel creation in Hub]]`.
2. Whether per-channel AES-GCM key derivation needs a bootstrap protocol revision
   bump in addition to the daemon protocol revision. Owned by this ticket,
   resolved during the `botster-hub-test-support` cutover. The control channel
   keeps its current unmodified derivation, so the published bootstrap protocol
   should not change; the implementer must confirm that before claiming it.
3. The exact placement of the reservation label on the admission response DTO —
   a new field on the existing attach result, or a new response body. Owned by
   this ticket, decided against `crates/botster-hub-client` prior art.

## 11. Acceptance checks and tests

Deterministic invariants gate every row. Wall-clock values are recorded only as
reference-runner evidence on `botster-ubuntu-24.04-16core`, never asserted.

| # | Invariant | Deterministic gate |
|---|---|---|
| A3 (Hub half) | `transport=duplex_binary` is required in Hello | wrong-token ablation fails compatibility with the typed diagnostic, proved through the Hello `required_features` exchange and the live Hub feature advertisement, not a `daemon-protocol.ts` grep, per `[[WebRTC adapter admission uses a Hello feature string not a generated DTO token]]` |
| A4 | one peer, one control channel, N subscription channels | assert channel count and exact labels for 3 concurrent terminal subscriptions |
| A5 (terminal half) | terminal output shares no queue with control | saturate the control channel, assert terminal frames still leave in order on their own channel. The entity half moves to `ticket_1787600682_233928` |
| A6 | a slow subscription does not block a sibling | hold one terminal channel at `bufferedAmount` high, assert a sibling terminal subscription still delivers byte-exact |
| A7 | section 9 limits are enforced from one table | the 33rd subscription is rejected with the typed error; no channel is created; the reserved count is unchanged |
| A8 | a late open on a suppressed generation binds no adapter | drive the production failure handler to retire the route, then fire the open. Assert Core terminal inventory holds zero routes for the grant. Red-on-revert: move the bind back into admission and assert this becomes the **first** failure |
| A8b | a `Reserved` route that never opens releases its slot | withhold the open past `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND`; assert the typed `subscription_channel_open_timeout` event and that the section 9 channel count returns to its pre-request value |
| A8c | both race orders are covered | run A8 in retire-then-open and open-then-retire order |
| A8d | an unreserved label binds nothing | the browser opens a well-formed label with no `Reserved` route; assert immediate close, no adapter, no charge |
| A8e | a duplicate open on a `Bound` route binds nothing | assert the second channel closes and the first route keeps delivering |
| A26 | the aggregate does not drift | saturate, drain fully, assert `aggregate_buffered()` returns to 0 and held classes resume. A stored counter fails this; the derived sum of section 9.2 passes |
| A27 | a refused terminal send is backpressure, not loss | the architecture section 14.3 A27 sequence T1 to T7, at most 8 attempts before the drain. Assert `WouldBlock` from `try_write`, `WouldBlock` from `pressure()`, Core retains the frame while the adapter does not, the aggregate stays at 2,097,152 B, and after draining below 1,048,576 B the same frame is delivered byte-exact with no duplicate |
| A27b | sustained aggregate pressure hits Core's hard stop | hold pressure through 512 consecutive unsuccessful attempts; assert Core `hard_stop`s the route, emits `ClientWorkerTeardown`, and Hub retires the route |
| A25 (aggregate half) | at the exact ceiling an admission is rejected on the aggregate, not the count | the section 14.3 setup built from terminal channels: 31 admitted while the aggregate is 0, filled to exactly 2,097,152 B, one free slot. Assert the aggregate-driven rejection, the aggregate unchanged at 2,097,152 B after it, the crossing send refused **before** the write, no sibling closed, and recovery to 1,998,848 B. The entity-overflow ordering arm moves to `ticket_1787600682_233928` |
| Enc | per-channel key derivation binds a frame to its channel | a frame captured on one subscription channel fails authentication when replayed on another |
| Chunk | binary chunking and reassembly are exact | a Core frame larger than `LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES` (12 KiB) reassembles byte-exact in order on the receiving peer |
| Ingress | terminal input reaches Core without a JSON round trip | N binary input frames reach `HubRuntime` through the subscription channel with zero pending control responses; `try_read` holds at least 64 complete frames before reporting `Lost` |
| Repl | replacement and reconnect | a replacement subscription with a higher generation binds; the stale generation's channel closes and never binds |

Section 15 characterization dispositions this ticket owns:

| Test | Disposition here |
|---|---|
| `webrtc_peer_rejects_a_second_data_channel` | rewritten to "rejects a second **unreserved** browser-created channel". It must keep the surviving-channel positive control from `[[rejected channel isolation needs a surviving channel positive control]]`: wait for a known terminal marker on the surviving channel, then require zero terminal frames on the rejected channel over the same window, and prove the rejected channel actually opened |
| `webrtc_peer_rejects_a_second_data_channel_requires_one_shot_claim` | rewritten for the control-channel-only claim |
| `webrtc_peer_post_handshake_data_channel_reaches_production_reject` | rewritten for reservation matching |
| `webrtc_shared_channel_carries_control_entity_event_and_terminal_frames` | the **terminal** arm is deleted here. Entity and event arms stay for `ticket_1787600682_233928` |
| `webrtc_ready_entity_frame_defers_terminal_output` | deleted; replaced by A5 and A6 |
| `terminal_adapter_contract_is_egress_only_at_the_locked_core_pin` | deleted; the rolled Core contract is duplex |
| `terminal_input_travels_as_a_json_control_request` | **kept** — the JSON route survives until `ticket_1787600679_990088` |
| `no_lua_dispatch_in_terminal_input_or_output` | kept, unchanged |
| `attach_ready_precedes_history_finish` | kept, unchanged |
| `shutdown_suppresses_exact_route_generations_before_core_teardown` | kept, unchanged |
| `webrtc_terminal_output_is_byte_exact` | kept; extended to the dedicated channel |
| `peer_close_leaves_sibling_peers_working` | kept; extended to sibling **channels** on one peer |
| `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` (`src/local_webrtc.rs:6505`) | kept; re-run per subscription channel, under a daemon child that inherits the injection environment per `[[Fault-injected WebRTC close requires a daemon started with the inject env]]` |
| `fair_write_class_coverage_per_transport` (`src/host_control_fair_write.rs:132`) | untouched; deleted with the file by `ticket_1787600682_233928` |

Live Hub proof, per `[[live hub proof records distinct hub and locked core binary provenance]]`:

1. Start from a fresh checkout at the exact Hub SHA under test.
2. `cargo build --locked -p botster-core-daemon --bin botster-session-worker`.
3. `cargo build --locked --bin botster-hub`.
4. Read the Core rev from that checkout's `Cargo.lock` and record it as a
   **distinct** identity from the Hub SHA.
5. Resolve both executable realpaths and require them under the fresh checkout's
   target directory.
6. Record the Hub SHA, the locked Core SHA, both build commands, and both
   resolved paths in the verification artifact.

### 11.1 Repository gate commands

The strict Rust gates are the repository's own CI gates
(`.github/workflows/ci.yml`), and the charter requires them. They run under the
**pinned toolchain**, Rust `1.97.0` with `rustfmt` and `clippy`, plus Zig
`0.16.0` for `botster-terminal-ghostty`'s `libghostty-vt` build. A different
local default toolchain is not the gate.

Commands, in this order, per
`[[Hub suite runs prebuild the session worker before the locked test wrapper]]`:

```sh
rustup toolchain install 1.97.0 --profile minimal --component rustfmt,clippy
zig version                                   # must report 0.16.0
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
cargo fmt --all -- --check                    # strict gate
cargo clippy --workspace --all-targets --locked -- -D warnings   # strict gate
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh --locked
(cd packages/hub-test-support && npm install --no-save && npm test)
```

Two commands in that list need their exact form justified, because the obvious
form of each does not work.

`./test.sh --workspace` is wrong. `test.sh` already passes `--workspace`, so the
flag arrives twice and Cargo aborts before any test runs.

`npm test` on its own is wrong. `packages/hub-test-support/package.json` declares
a runtime dependency on `@trybotster/ui-contract@0.3.3`, and `node_modules` is
gitignored and absent from a fresh checkout. Measured at base `55f620d`:
`npm test` aborts with `ERR_MODULE_NOT_FOUND: Cannot find package
'@trybotster/ui-contract'` before a single assertion runs, so it can never fail
on a contract regression — it fails on resolution first. With
`npm install --no-save` in front, the same command reports `hub test-support
package import and fixture materialization passed`. `--no-save` is load-bearing:
plain `npm install` writes an untracked `package-lock.json`, and no
`package-lock.json` is tracked anywhere in this repository, so `npm ci` is not
available either.

`cargo check --workspace --all-targets` stays useful during development but is
**not** a substitute for the two strict gates, and the plan no longer lists it as
the strict Rust gate.

### 11.2 Pre-existing baseline failures, owned by a prerequisite ticket

The strict gates are **both red on `main`** before this ticket changes anything.
Measured at base `55f620d` under the CI-pinned toolchain `rustc 1.97.0
(2d8144b78 2026-07-07)` with Zig `0.16.0`:

| Gate | Result | Location |
|---|---|---|
| `cargo fmt --all -- --check` | exit 1, one file | `src/local_webrtc.rs:7710`, inside `post_handshake_data_channel_opens_and_delivers_bytes`, the test added by the merged prerequisite `ticket_1787654915_646236` |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 101, one distinct error | `src/package_entity_fanout.rs:515`, `clippy::collapsible_match` |

**Neither repair belongs in this ticket.** Blocking question
`question_1787667127_613797` resolved this: `ticket_1787667162_566252`, "Hub:
restore the strict Rust gate baseline", owns both. It formats only the merged
post-handshake WebRTC test, fixes only the `package_entity_fanout`
collapsible-match warning, forbids behavior changes, adjacent cleanup, and module
extraction, and must pass `cargo fmt`, strict clippy, and `./test.sh --locked`.

The first draft of this plan proposed fixing the `fmt` failure here, on the
grounds that `src/local_webrtc.rs` is a file this ticket rewrites. That was
overruled, and the instruction is explicit: **do not carry either baseline repair
in the feature diff.** Implement starts from a `main` on which both gates are
already green, so any strict-gate failure it sees is its own.

This is why the clippy failure mattered enough to stop for. It sits in
`src/package_entity_fanout.rs`, a file this ticket lists as untouched by design,
so a waiver would have left this ticket unable to distinguish an inherited
failure from a regression in its own new module.

### 11.3 Downstream consumer proof for the public DTO change

Scope item 11 changes public `botster-hub-client` DTOs — the reservation label and
generation on the admission response, and the typed
`subscription_channel_open_timeout` host event. Those reach the generated
TypeScript, so `[[hub generated protocol changes are a four site release chain]]`
applies in full. The four sites, in order:

1. `crates/botster-hub-client/src/typescript.rs` and its checked-in
   `crates/botster-hub-client/generated/daemon-protocol.ts`.
2. `npm run sync` in `packages/hub-test-support`, updating the mirrored
   `daemon-protocol.ts` and `metadata.json` and bumping `package.json`.
   `node scripts/sync-assets.mjs --check` detects stale assets, and
   `[[Hub test support version bumps must update the Node mirror test literals]]`
   requires the separate `test.mjs` literals plus `npm test`.
3. Publishing a new `@trybotster/hub-test-support` coordinate.
4. The consumer's vendored copy and exact pin.

Measured current values at base `55f620d`: `packages/hub-test-support` is
`0.1.42`, `metadata.json` reports `protocol_version` 7 and
`conformance_fixture_revision` 46, and `ui_contract.package_version` is `0.3.3`.
This ticket moves the test-support package to the next unpublished version and
bumps the conformance fixture revision. The UI contract is unchanged, because
this ticket adds no new UI types.

**Sites 1 and 2 only are in this ticket's scope.** Site 3 is a separate human
publish action, so this ticket produces an **unpublished** coordinate per
`[[Hub test support capability cutovers use a new unpublished package version]]`,
and neither Implement nor Verify may claim a release. Site 4 belongs to the
downstream consumer tickets. Per
`[[closed dependency tickets signal merged source not a consumable release]]`,
merging this ticket does not make the coordinate consumable; the downstream Web
ticket stays blocked until the coordinate is published and inspected.

Both downstream shapes must still be measured here, because
`[[a ui contract import line change costs one test line in each generic client]]`
shows production builds staying green while each generic client breaks on exactly
one test line:

**TUI-shaped Cargo proof.** Per
`[[tui shaped Hub consumer proofs must include hub test support]]`, a scratch
consumer declares all three Hub dependencies — `botster-hub-client` and
`botster-ui-contract` as normal dependencies and `botster-hub-test-support` as a
dev-dependency — and builds with `cargo build --tests`, so Cargo compiles the
dev edge. Inspect it with
`cargo tree -i botster-ui-contract -e normal,dev`; without `-e normal,dev` a
second contract identity reachable only through the test-support crate stays
invisible. A client-only probe is not TUI-shaped and is rejected. The DTO
additions are source-breaking for any complete struct literal in a consumer test
helper, which is the `E0063` cost that only `--all-targets` exposes.

**Web-shaped npm proof.** Pack the new coordinate and install the tarball into a
clean scratch consumer, then resolve through exported roots rather than
`package.json`, per
`[[clean consumer smokes resolve exported root entrypoints not package json]]`,
and assert the new protocol tokens in the installed tree. Assert the exact
generated import line, which is the pinned assertion that moves when the emitter
output changes.

**Ablation.** Per `[[a Cargo source identity proof needs a wrong tag ablation]]`,
the Cargo identity claim needs a matching-rev green arm and a wrong-rev arm that
fails to compile. An identity oracle that cannot go red proves nothing.

## 12. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| The Core pin roll is itself a large breaking change landing inside a large ticket | review surface widens; a Core regression is attributed to channel work | commit 1 is the roll alone and must be green on its own before commit 3 starts |
| Ticket size. Core roll, a new module, adapter ingress, a protocol cutover, and fifteen-plus acceptance rows in one Implement step | review fatigue; partial delivery | the six-commit sequence in section 9 keeps each commit reviewable; the move-only commit is separate by project rule |
| A channel opens after its subscription retired | a resurrected route leaks a Core adapter | A8 with a red-on-revert control, plus A8c for both race orders |
| An unreserved or duplicate label binds an adapter | fail-open admission | A8d and A8e; a label with no `Reserved` route closes immediately and charges nothing |
| Extraction becomes a file-only split | `[[Hub extraction must reduce ownership rather than only split files]]` fails at review | architecture section 12.1 assigns responsibilities, not line counts; no forwarding wrapper is left in `src/local_webrtc.rs` |
| 33 channels per peer exceed a browser or `webrtc-rs` limit | admission fails late instead of at the table | A7 tests the boundary; unknown 1 measures it early |
| Per-channel AES-GCM adds a copy per terminal frame | throughput regression on large history | reference-runner comparison against the post-Restty baseline; recorded as evidence, never asserted |
| Sustained aggregate saturation tears down a terminal route after 512 consecutive `WouldBlock` results | backpressure silently becomes route teardown | the implementer accepts this deliberately; A27b proves the documented end state; section 9.1 excluding control from the budget keeps the teardown notice sendable |
| Semantic rebase against `ticket_1787603671_590198` and `ticket_1787600682_233928` | completed review goes stale | disjoint new modules; this ticket merges first; review is renewed after any semantic rebase |
| Wall-clock assertions flake under suite load | false failures | deterministic gates only; timing recorded as evidence |
| Both strict gates are already red on `main` (`fmt` at `src/local_webrtc.rs:7710`, `clippy` at `src/package_entity_fanout.rs:515`) | an inherited failure is mistaken for one this ticket caused, or a strict gate is skipped as "known broken" | `ticket_1787667162_566252` owns both repairs and blocks this ticket; neither repair may appear in this feature diff; Implement starts from a green baseline |
| The strict gates are omitted or run under the wrong toolchain | `clippy -D warnings` and `rustfmt` differ across Rust versions, so a local pass is not a CI pass | section 11.1 pins Rust `1.97.0` and Zig `0.16.0` and lists both strict gates explicitly |
| `npm test` is listed without `npm install --no-save` | the Node acceptance command aborts on module resolution and can never fail on a contract regression, so it reads as passing coverage that does not exist | section 11.1 records the measured `ERR_MODULE_NOT_FOUND` and the working form |
| The public DTO change ships without downstream-shaped proof | production builds stay green while each generic client breaks on one test line | section 11.3 requires the TUI-shaped three-dependency `cargo build --tests` probe with a wrong-rev ablation and the Web-shaped packed-tarball probe |
| Site 3 publication is claimed rather than performed | a downstream ticket consumes a coordinate that does not exist or carries stale bytes | section 11.3 scopes this ticket to sites 1 and 2 and forbids any release claim |
| The rejected-channel test stays tautological | isolation is asserted, not measured | the surviving-channel positive control and the channel-`Open` proof are both required, per `[[rejected channel isolation needs a surviving channel positive control]]` |

## 12.1 Plan Review return, review_1787666788_871227

Verdict was `changes_required` with four findings. Each was re-measured at base
`55f620d` before it was accepted, per the role rule that a reported failure needs
exact evidence rather than agreement.

| Finding | Verdict after independent check | Resolution |
|---|---|---|
| `finding_1787666789_283053` — strict Rust gates omitted, touched-file baseline failure missed | **Correct, and worse than reported.** `.github/workflows/ci.yml` runs `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings` under Rust `1.97.0` and Zig `0.16.0`, and the plan listed neither. Re-measuring under the pinned toolchain found **two** baseline failures, not one: `fmt` at `src/local_webrtc.rs:7710` and `clippy::collapsible_match` at `src/package_entity_fanout.rs:515` | Section 11.1 adds the pinned toolchain and both strict gates. Section 11.2 records both failures and assigns both repairs to the new prerequisite `ticket_1787667162_566252`, per the answer to `question_1787667127_613797` |
| `finding_1787666789_115871` — the Node acceptance command cannot reach test execution | **Correct.** `npm test` aborts with `ERR_MODULE_NOT_FOUND: Cannot find package '@trybotster/ui-contract'` before any assertion, so it could never fail on a contract regression | Section 11.1 requires `npm install --no-save && npm test`, measured green, and `--no-save` keeps `git status --porcelain` empty because no `package-lock.json` is tracked |
| `finding_1787666789_589917` — public DTO and test-support changes lack downstream proof | **Correct.** The reservation label, generation, and open-timeout event reach the generated TypeScript, so the four-site release chain applies in full | Section 11.3 adds the chain, the TUI-shaped three-dependency `cargo build --tests` probe with `cargo tree -i botster-ui-contract -e normal,dev`, the wrong-rev ablation, the Web-shaped packed-tarball probe, and the rule that this ticket owns sites 1 and 2 only |
| `finding_1787666789_591438` — plan completion evidence empty, checklist items pending | **Half correct.** "Checklist items pending" is true and is fixed. "Completion evidence is empty" is a visibility artifact, not a missing submission: `gate.submitted` for `botster_stack_plan_gate` recorded `status: passed` with the full evidence object, but `request_step_advance` was called without `evidence`, so `step.completed` recorded `evidence: {}` — the record a reviewer reads. The reviewer classified it process-only, which is right | Checklist items are completed with evidence, and the evidence object is passed to `request_step_advance` as well as `submit_gate` |

The reviewer's routing, charter, ownership, dependency, and runtime-teardown
conclusions matched this plan, so nothing in sections 1 to 10 changed.

## 13. Vault gaps worth capturing

1. **The `botster-hub-playbook` required gate still says "While Hub pins
   `webrtc 0.20`, reject Hub-created post-handshake channels".** That pin is
   merged away at base `55f620d`. The gate line needs the 0.21 wording. This is
   the highest-value gap, because a future planner reading the charter would
   plan against a limitation that no longer exists.
2. **`[[Hub Core pin rolls update eleven literal sites and six lock sources]]`
   undercounts.** The measured active literal count at this base is eighteen.
   The note should state the discovery command rather than a fixed count.
3. **A subscription channel label binds identity and generation.** The
   architecture section 8.3 scheme deserves an atomic note once implemented.
4. **Per-channel AES-GCM binds a frame to its subscription.** The section 8.4
   derivation change and its replay-resistance rationale.
5. **Hub content-blindness permits transport framing.** Chunking and encrypting
   an opaque byte string does not violate content-blindness. The existing note
   does not say so.
6. **Reserve at admission, bind at open.** The two-phase split is what makes the
   late-open guard reachable at all. Capture after this ticket proves it.
7. **A Node package gate can abort before its first assertion.**
   `packages/hub-test-support` declares a runtime dependency and gitignores
   `node_modules`, so a bare `npm test` fails on module resolution and reads as a
   real gate while proving nothing. `npm install --no-save` is the form that both
   runs the assertions and leaves the tracked worktree clean. Worth an atomic
   note, because a plan listing `npm test` alone looks correct.
8. **The Hub strict Rust gates are the CI gates, under a pinned toolchain.**
   `[[botster-hub-playbook]]` says "strict Rust gates" without naming
   `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked
   -- -D warnings`, Rust `1.97.0`, or Zig `0.16.0`. Naming them in the charter
   would have prevented this Plan return.
