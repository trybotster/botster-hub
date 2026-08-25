# Isolate control and terminal subscriptions on dedicated DataChannels

Ticket: `ticket_1787600674_500120`
Run: `run_1787678814_340532`. It replaces the cancelled runs
`run_1787653825_278029` and `run_1787664777_379002`.
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Base commit: `a0c7141` on `main`. It is the merge of
`ticket_1787667162_566252`, "Hub: restore the strict Rust gate baseline", and it
contains the merged `webrtc 0.21.0-beta.2` roll from `55f620d`.

## 1. Target

- Target repository: `botster-hub` (`https://github.com/trybotster/botster-hub.git`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Project: `project_1787600579_585482`, Botster Isolated Subscription Data Plane

The target id was resolved through `project_pipelines_get_project`. Every Hub
ticket in this project carries the same target id. The ambient worktree was not
used to choose the repository.

Registered dependencies, all three closed:

| Dependency | Ticket | Status |
|---|---|---|
| `dependency_1787600712_947298` | `ticket_1787600672_342292` — Core: make terminal subscriptions duplex and pressure-isolated | closed |
| `dependency_1787654923_752279` | `ticket_1787654915_646236` — Hub: upgrade WebRTC for post-handshake DataChannel creation | closed |
| `dependency_1787667169_738534` | `ticket_1787667162_566252` — Hub: restore the strict Rust gate baseline | closed, merged as `a0c7141` |

No blocking dependency remains. `project_pipelines_current_context` reports
`blocking_dependencies` empty for this run.

The third dependency was registered during the previous Plan visit, after the
strict Rust gates were measured red on `main`. That prerequisite is now merged.
Section 11.2 records the re-measured, now green, baseline. This plan carries no
part of either baseline repair, exactly as the answer to
`question_1787667127_613797` required.

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

Release-chain and downstream-proof notes. Section 11.3 applies all six. They were
cited there but omitted from this inventory in the previous revision:

- `[[hub generated protocol changes are a four site release chain]]`
- `[[closed dependency tickets signal merged source not a consumable release]]`
- `[[a ui contract import line change costs one test line in each generic client]]`
- `[[tui shaped Hub consumer proofs must include hub test support]]`
- `[[clean consumer smokes resolve exported root entrypoints not package json]]`
- `[[a Cargo source identity proof needs a wrong tag ablation]]`

That makes **34** targeted atomic notes: 28 above plus these 6. The previous
revision's artifact and checklist evidence said 29, which was wrong in both
directions — it overcounted the first list and omitted the second.

Gate-hygiene notes added during this Plan visit, all published by the merged
prerequisite:

- `[[Hub official gates must not set CARGO TARGET DIR]]`
- `[[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]`
- `[[strict clippy can hide later crate diagnostics behind the first compile failure]]`
- `[[test script required for rust tests not cargo test]]`
- `[[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]`

`[[project-pipelines-playbook]]` is not loaded. No Project Pipelines package or
plugin path is in scope.

## 3. Context loaded

Repository sources read at base `a0c7141`:

- `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md`
  — the frozen architecture contract, sections 8 to 18.
- `src/local_webrtc.rs` (7871 lines at `a0c7141`) — `on_data_channel` at `:1148`,
  `run_data_channel`, `send_text_or_peer_terminal` at `:1203`, constants at
  `:50-70`, `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`
  at `:6505`, `claim_data_channel` at `:935`,
  `LOCAL_WEBRTC_CHUNK_PAYLOAD_BYTES` at `:54`, `run_data_channel` at `:1269`.
  Every one of these line numbers was re-measured at `a0c7141`. The
  prerequisite's rustfmt repair removed four lines, all after `:7710`, so no
  reference above `:7710` moved.
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

- `question_1787665047_404406` — options 1A and 2A confirmed. The owner recorded
  the same split on both owning ticket descriptions. Section 10 carries both.
- `question_1787667127_613797` — option A confirmed with one adjustment. One
  prerequisite ticket owned **both** strict-gate failures, not the clippy one
  alone, because `main` also failed `cargo fmt`. That ticket is merged.

No blocking question is open for this run. This plan asks none, because every
ambiguity the previous visits found is now answered and recorded above.

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
| **`Reserved` route that never opens (new surface)** | the same triple | `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` expires | **No channel exists, so Hub closes nothing.** Hub retires the `Reserved` route, releases its section 9 slot, and emits `subscription_channel_open_timeout` on the control channel. Without the release the slot leaks against the channel count |
| **Unreserved browser-created channel (new surface)** | none — no route matches its label | closed immediately, fail closed. A label with no `Reserved` route binds nothing and charges nothing | none needed; nothing was created |
| Any peer-originated `DaemonRequest` on the control channel | `grant_id` | `local_webrtc_peer_gone_request_error` | none needed |

Bounded `DataChannel` close applies only where a channel actually exists: a late,
stale, unreserved, duplicate, mismatched, or over-limit **open event**, and peer
teardown. The open-timeout path has no channel to close, because admission
creates none and the browser never created one. The previous revision said Hub
closes the channel on timeout. That was a residue of the superseded
Hub-creates-every-channel contract in architecture section 8.2, and it is
corrected here and in row A8b.

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
eleven literal sites. The count re-measured at `a0c7141` is **eighteen active
sites**, so the implementer must use the measured set and not the note's count.

The discovery command is authoritative, not the count:

```sh
grep -rn '7eafa470a18025895995bbedc20d34b58106a03b' \
  --include='*.rs' --include='*.toml' --include='*.json' --include='*.mjs' . \
  | grep -v '^./target/' | grep -v '^./docs/'
```

At `a0c7141` that command returns nineteen lines. Eighteen are active sites.
The nineteenth is
`docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`,
a historical evidence record that must **not** be rolled. The `--include='*.json'`
filter is why it appears at all; keep the filter and exclude the file by name.

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

`botster-core` `origin/main` was re-resolved during this Plan visit with
`git ls-remote https://github.com/trybotster/botster-core.git refs/heads/main`.
It still reports `358ef1a6bf0f792f6da10d60890be39cb16779d0`, unchanged from the
previous visit, so the roll target is confirmed rather than assumed stale.

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
| A8b | a `Reserved` route that never opens releases its slot | withhold the open past `LOCAL_WEBRTC_CHANNEL_OPEN_BOUND`; assert the typed `subscription_channel_open_timeout` event and that the section 9 channel count returns to its pre-request value. Assert Hub issues **no** `DataChannel::local_close()` on this path, because no channel exists to close |
| A8c | both race orders are covered | run A8 in retire-then-open and open-then-retire order |
| A8d | an unreserved label binds nothing | the browser opens a well-formed label with no `Reserved` route; assert immediate close, no adapter, no charge |
| A8e | a duplicate open on a `Bound` route binds nothing | assert the second channel closes and the first route keeps delivering |
| A8f | a **partially** matching label cannot claim a reservation | hold exactly one `Reserved` route, then open one channel per mismatch axis of the section 8.3 label: wrong channel kind, wrong `session_id`, wrong `subscription_id`, and wrong `generation`, each with every other field correct. For each arm assert the channel reaches `Open`, Hub closes it, no Core adapter is bound, section 9 accounting is unchanged, and the one `Reserved` route is **still** `Reserved` and still bindable by its exact label afterwards. A single all-fields-wrong label is not a substitute: it cannot distinguish a per-field comparison from a whole-string comparison that happens to reject |
| A8g | **externally visible outcome only.** A full connection rejects an extra opened channel and no sibling suffers | fill the connection to `MAX_TOTAL_CHANNELS` = 33, one control channel plus 32 `Bound` subscription channels. The browser opens one extra reliable ordered channel. Assert it reaches `Open`, binds no adapter, is closed by Hub, leaves the section 9 counts at 33 and the aggregate unchanged, and does not disturb a surviving `Bound` channel, proved by a known terminal marker delivered byte-exact on that sibling over the same window per `[[rejected channel isolation needs a surviving channel positive control]]`. **Assert the typed rejection reason equals the unreserved reason, not the limit reason.** This row does **not** prove the limit guard, and section 11.0 says why. A7 does not cover it either: A7 rejects the 33rd **admission** before any channel exists |
| A8h | **the open-time limit guard is load-bearing.** A reserved, identity-matching open event is refused when the connection is **over** its charged limit | a focused state-construction unit lane, not a production browser flow. Build mux state directly: **32 `Bound` subscription routes plus one matching `Reserved` route, so the charged subscription count is 33.** The count **includes** the matching `Reserved` route, because section 9 charges the slot at `Reserved`, and 33 is strictly greater than `MAX_SUBSCRIPTION_CHANNELS` = 32. The `Reserved` route's section 8.3 label, session, subscription, and generation all match the open event. Drive the **production** open-event decision function against that state. Assert it refuses, binds no Core adapter, emits the typed **limit** reason distinct from the unreserved reason, and leaves the counts unchanged. **Red-on-revert, and it must be assertion-specific:** delete only the greater-than-maximum predicate, leaving every other guard in place, and assert A8h becomes the **first** failure, per `[[a regression test must be shown to go red with the fix reverted]]`. Identity, generation, route state, duplicate, and unreserved guards all pass on this input by construction, so the limit predicate is the only check that can refuse it |
| A8i | **positive boundary.** At exactly the maximum, a matching reservation still binds | the same state-construction lane, one route lower: **31 `Bound` subscription routes plus one matching `Reserved` route, so the charged subscription count is exactly 32**, again counting the matching `Reserved` route. Drive the same production decision function and assert it **binds** the Core adapter and moves the route to `Bound`. This row is the reason A8h cannot be written at count 32: `MAX_SUBSCRIPTION_CHANNELS` = 32 **permits** 32 charged subscription routes, so the 32nd subscription is valid and must succeed. A8i fails if the implementation writes the predicate as `>=` instead of `>`, which is the exact defect an at-limit A8h would have taught |
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

### 11.0 Why the open-time limit check needs two rows

Admission charges the section 9 limit table and only then inserts a `Reserved`
route. A `Reserved` route therefore implies its charge already succeeded. Two
consequences follow, and they are the reason A8g and A8h are separate rows with
separate stated proof roles.

**On the production path, an over-limit open event carrying a valid reservation
is unreachable.** At a full connection the extra channel the browser opens has no
reservation at all, so the unreserved guard refuses it before any limit predicate
runs. That is correct fail-closed behavior, and it is what A8g measures.

**So A8g cannot prove the limit guard.** Delete the open-time limit check and A8g
still passes, because the unreserved guard was doing the work. The previous
revision of this plan claimed A8g proved open-time limit rejection. It did not,
and Plan Review `review_1787681114_793607` was right to reject that claim.

The open-time limit check is therefore **defensive**: it refuses a reserved,
identity-matching open event whose connection is **over** its charged limit, a
state production admission should never produce.

**The boundary is strict, and the plan states it once here.** Section 9 charges
the subscription slot when the route becomes `Reserved`, and
`MAX_SUBSCRIPTION_CHANNELS` = 32 **permits** 32 charged subscription routes. A
charged count of 32 is therefore **at** the limit and must bind; only a count
strictly greater than 32 is over it. The predicate is `> 32`, never `>= 32`. Any
constructed over-limit state must say whether its count includes the matching
`Reserved` route; A8h and A8i both include it.

Revision 3 of this plan got that boundary wrong. A8h constructed a charged count
of 32 and expected a refusal, which would have driven an implementation that
rejects the valid 32nd subscription. Plan Review
`review_1787681637_806331` caught it. A8i now pins the other side of the
boundary so the error cannot return. The ticket requires the check —
"Reject late, stale, mismatched, duplicate, unreserved, **or over-limit** open
events" — so the check ships, and a defensive check still needs a test that goes
red when it is removed. A8h is that test, and it must construct the state
directly, because no production sequence reaches it.

The two rows carry different burdens, and neither substitutes for the other:

| Row | Proof role | Path | Red-on-revert target |
|---|---|---|---|
| A8g | the externally visible rejection and sibling isolation at a full connection | production browser flow | none; it is deliberately not the limit guard's proof |
| A8h | the open-time limit predicate itself, above the maximum | state-construction unit lane, 32 `Bound` plus 1 matching `Reserved` = 33 charged | delete only the greater-than-maximum predicate; A8h must fail first |
| A8i | the boundary below it: at exactly the maximum a matching reservation binds | same lane, 31 `Bound` plus 1 matching `Reserved` = 32 charged | none; it is the positive control that forbids `>=` |

This split requires the rejection reasons to be **distinguishable typed values**,
not one generic close. A8g asserts the unreserved reason and A8h asserts the
limit reason. Without distinct reasons neither row can name which guard refused
the channel, and A8g would silently reacquire the false claim this section
removes.

### 11.1 Repository gate commands

The strict Rust gates are the repository's own CI gates
(`.github/workflows/ci.yml`), and the charter requires them. They run under the
**pinned toolchain**, Rust `1.97.0` with `rustfmt` and `clippy`, plus Zig
`0.16.0` for `botster-terminal-ghostty`'s `libghostty-vt` build. A different
local default toolchain is not the gate.

The merged prerequisite `ticket_1787667162_566252` published the exact official
gate block for this repository. This plan adopts it verbatim and does not invent
a second form. Commands, in this order, per
`[[Hub suite runs prebuild the session worker before the locked test wrapper]]`:

```sh
export RUSTUP_TOOLCHAIN=1.97.0
unset CARGO_TARGET_DIR
rustc --version                     # must print 1.97.0
zig version                         # must print 0.16.0
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
cargo fmt --all -- --check                                       # strict gate
cargo clippy --workspace --all-targets --locked -- -D warnings   # strict gate
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh --locked
(cd packages/hub-test-support && npm install --no-save && npm test)
git diff --check a0c7141...HEAD
```

Four lines in that block need their exact form justified, because the obvious
form of each does not work.

`export RUSTUP_TOOLCHAIN=1.97.0` is load-bearing. Per
`[[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]`,
a pipeline agent shell can select Rust `1.92.0` while CI pins `1.97.0`. A bare
strict clippy then exits `0` on code that CI rejects. The Implementer must print
`rustc --version` from the same shell as the gates and attach that line.

`unset CARGO_TARGET_DIR` is load-bearing and it **overrides** the generic
pipeline hygiene advice to redirect the Cargo target directory. Per
`[[Hub official gates must not set CARGO TARGET DIR]]`, official locked gates
must use the default worktree `target/` directory, because two override classes
break `./test.sh --locked` for reasons unrelated to this ticket:

- An out-of-worktree directory fails the spawn census.
  `executable_from_this_worktree` accepts only a `botster-session-worker` whose
  argv0 starts with `CARGO_MANIFEST_DIR`.
- A non-default in-worktree directory fails
  `update_replaces_the_daemon_before_a_verification_failure`, because
  `src/update.rs` honors `CARGO_TARGET_DIR` while `tests/update_command_test.rs`
  hard-codes `target/debug/botster-session-worker`.

The generic hygiene rule that motivates a redirect applies only to a worktree
path that contains `:`. This run's worktree path is
`.../trybotster-botster-hub-project-pipelines-ticket_1787600674_500120`, which
contains no colon, so the two rules do not actually conflict here. If a future
run of this ticket lands on a colon path, the repository rule wins and the run
must relocate the worktree rather than set `CARGO_TARGET_DIR`.

Also, after every strict clippy repair the Implementer must rerun the whole
clippy gate, per
`[[strict clippy can hide later crate diagnostics behind the first compile failure]]`.
A single failing compile unit hides diagnostics in later crates, so one green
rerun is not evidence until it follows the last repair.

`./test.sh --workspace` is wrong. `test.sh` already passes `--workspace`, so the
flag arrives twice and Cargo aborts before any test runs.

`npm test` on its own is wrong. `packages/hub-test-support/package.json` declares
a runtime dependency on `@trybotster/ui-contract@0.3.3`, and `node_modules` is
gitignored and absent from a fresh checkout. Re-measured at base `a0c7141` under
Node `v22.21.1`: `npm test` aborts with `ERR_MODULE_NOT_FOUND: Cannot find
package '@trybotster/ui-contract'` before a single assertion runs, so it can
never fail on a contract regression — it fails on resolution first. With
`npm install --no-save` in front, the same command exits `0` and reports
`hub test-support package import and fixture materialization passed`.
`git status --porcelain` stayed empty afterwards, which is the tracked-worktree
requirement. `--no-save` is load-bearing:
plain `npm install` writes an untracked `package-lock.json`, and no
`package-lock.json` is tracked anywhere in this repository, so `npm ci` is not
available either.

`./test.sh --locked` is the Rust test gate. A bare `cargo test` is not a
substitute, per `[[test script required for rust tests not cargo test]]`, because
`test.sh` first runs
`node packages/hub-test-support/scripts/sync-assets.mjs --check` and then passes
`--workspace` itself.

`cargo check --workspace --all-targets` stays useful during development but is
**not** a substitute for the two strict gates, and the plan no longer lists it as
the strict Rust gate.

Reviewers must read raw Cargo output for gate evidence, not an `rtk` summary, per
`[[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]`.

### 11.2 Baseline re-verification at `a0c7141`, independently measured

The previous Plan visit measured **both** strict gates red on `main` at base
`55f620d`. `ticket_1787667162_566252` owned both repairs and merged as `a0c7141`.

This Plan visit re-measured the baseline itself rather than trusting the
prerequisite's own report. Measurements were taken in this run's worktree with
`RUSTUP_TOOLCHAIN=1.97.0` exported and `CARGO_TARGET_DIR` unset, on the branch
rebased onto `a0c7141`:

| Check | Command | Result at `a0c7141` |
|---|---|---|
| toolchain | `rustc --version` | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| Zig | `zig version` | `0.16.0` |
| format | `cargo fmt --all -- --check` | exit `0` |
| lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit `0`, zero warning or error lines |
| worktree | `git status --porcelain` | empty |
| tracked `.gitignore` | `wc -c .gitignore` | 53 bytes, matches `HEAD` |
| Core roll target | `git ls-remote .../botster-core.git refs/heads/main` | `358ef1a6bf0f792f6da10d60890be39cb16779d0`, unchanged |

Both strict gates are therefore green before this ticket changes anything.
**Any strict-gate failure the Implement step sees is its own regression**, and it
may not be attributed to an inherited baseline.

Two consequences follow, and both are binding:

1. Neither baseline repair may appear in this ticket's diff. The rustfmt repair
   inside `post_handshake_data_channel_opens_and_delivers_bytes` and the
   `clippy::collapsible_match` repairs in `src/package_entity_fanout.rs` and
   `tests/hub_daemon_lifecycle/sessions.rs` are already on `main`. This is the
   explicit instruction from the answer to `question_1787667127_613797`.
2. `src/package_entity_fanout.rs` stays on this ticket's untouched list. The
   earlier reason for touching it is gone.

This is why the earlier clippy failure was worth stopping for. It sat in a file
this ticket lists as untouched, so a waiver would have left this ticket unable to
tell an inherited failure from a regression in its own new module.

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

Re-measured at base `a0c7141`, all unchanged from the previous visit:
`packages/hub-test-support/package.json` is `0.1.42`, `metadata.json` reports
`package_version` `0.1.42`, `protocol_version` 7 and
`conformance_fixture_revision` 46, `ui_contract.package_version` is `0.3.3`, and
`crates/botster-hub-client/src/lib.rs` declares `PROTOCOL_VERSION = 7` at `:33`
and `CONFORMANCE_FIXTURE_REVISION = 46` at `:34`.
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
| A guard is proved by a test that an earlier guard already satisfies | the check can be deleted with every test still green, so it is dead in practice | section 11.0 splits A8g from A8h, requires distinguishable typed rejection reasons, and gives A8h an assertion-specific red-on-revert that removes only the greater-than-maximum predicate |
| An over-limit acceptance row is constructed at the limit instead of above it | the row drives the implementation to reject the last **valid** subscription, turning a defensive check into a live product defect | section 11.0 states the strict boundary once: the slot is charged at `Reserved`, 32 charged routes are permitted, the predicate is `> 32` and never `>= 32`. A8h constructs 33 and A8i pins 32 as a binding case |
| Extraction becomes a file-only split | `[[Hub extraction must reduce ownership rather than only split files]]` fails at review | architecture section 12.1 assigns responsibilities, not line counts; no forwarding wrapper is left in `src/local_webrtc.rs` |
| 33 channels per peer exceed a browser or `webrtc-rs` limit | admission fails late instead of at the table | A7 tests the boundary; unknown 1 measures it early |
| Per-channel AES-GCM adds a copy per terminal frame | throughput regression on large history | reference-runner comparison against the post-Restty baseline; recorded as evidence, never asserted |
| Sustained aggregate saturation tears down a terminal route after 512 consecutive `WouldBlock` results | backpressure silently becomes route teardown | the implementer accepts this deliberately; A27b proves the documented end state; section 9.1 excluding control from the budget keeps the teardown notice sendable |
| Semantic rebase against `ticket_1787603671_590198` and `ticket_1787600682_233928` | completed review goes stale | disjoint new modules; this ticket merges first; review is renewed after any semantic rebase |
| Wall-clock assertions flake under suite load | false failures | deterministic gates only; timing recorded as evidence |
| A baseline repair is re-carried into this feature diff | the review surface widens and the diff contains work `main` already has | `ticket_1787667162_566252` merged as `a0c7141`; section 11.2 re-measures both strict gates green; `src/package_entity_fanout.rs` returns to the untouched list |
| A strict gate runs under a pipeline shell toolchain below the CI pin | a bare clippy exits `0` on code CI rejects | the gate block exports `RUSTUP_TOOLCHAIN=1.97.0` and requires the `rustc --version` line captured in the same shell |
| A gate run sets `CARGO_TARGET_DIR` | `./test.sh --locked` fails the spawn census or the update test, and the failure teaches the wrong lesson | the gate block unsets it; section 11.1 records why the generic hygiene rule does not apply on this colon-free worktree path |
| One clippy repair hides diagnostics in later crates | a partial green reads as a full green | the whole clippy gate reruns after the last repair |
| The strict gates are omitted or run under the wrong toolchain | `clippy -D warnings` and `rustfmt` differ across Rust versions, so a local pass is not a CI pass | section 11.1 pins Rust `1.97.0` and Zig `0.16.0` and lists both strict gates explicitly |
| `npm test` is listed without `npm install --no-save` | the Node acceptance command aborts on module resolution and can never fail on a contract regression, so it reads as passing coverage that does not exist | section 11.1 records the measured `ERR_MODULE_NOT_FOUND` and the working form |
| The public DTO change ships without downstream-shaped proof | production builds stay green while each generic client breaks on one test line | section 11.3 requires the TUI-shaped three-dependency `cargo build --tests` probe with a wrong-rev ablation and the Web-shaped packed-tarball probe |
| Site 3 publication is claimed rather than performed | a downstream ticket consumes a coordinate that does not exist or carries stale bytes | section 11.3 scopes this ticket to sites 1 and 2 and forbids any release claim |
| The rejected-channel test stays tautological | isolation is asserted, not measured | the surviving-channel positive control and the channel-`Open` proof are both required, per `[[rejected channel isolation needs a surviving channel positive control]]` |

## 12.1 Plan Review return from the previous visit, review_1787666788_871227

This section is history. It records the four findings the previous Plan visit
received on run `run_1787664777_379002`, and how each was resolved. Every
resolution survives into this revision.

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

Two gaps from the previous visit are now partly closed. This revision re-checked
the charter rather than repeating the earlier claim.

1. **`[[botster-hub-playbook]]` line 180 still says "While Hub pins `webrtc
   0.20`, reject Hub-created post-handshake channels and require every isolation
   test channel to prove `Open`".** That pin is merged away. **Partly closed:**
   line 179 was already corrected to "reserve the label in Hub and let the
   browser create the channel", which is this ticket's contract. Line 180's
   `0.20` clause is the remaining stale half. The `Open` proof requirement is
   still correct and must survive any rewrite. This stays the highest-value gap,
   because a planner reading line 180 alone would plan against a limitation that
   no longer exists.
2. **`[[Hub Core pin rolls update eleven literal sites and six lock sources]]`
   undercounts.** The re-measured active literal count at `a0c7141` is eighteen,
   plus one historical `docs/reports/**` match that must not be rolled. The note
   should state the discovery command and the `docs/` exclusion rather than a
   fixed count.
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
   **Partly closed.** `[[botster-hub-playbook]]` now carries
   `[[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]`
   at line 43 and the `RUSTUP_TOOLCHAIN=1.97.0` rule at line 207, and
   `[[Hub official gates must not set CARGO TARGET DIR]]` exists. The charter
   still never writes the two gate command strings themselves, so a planner must
   read `.github/workflows/ci.yml` to learn them. One line naming
   `cargo fmt --all -- --check` and
   `cargo clippy --workspace --all-targets --locked -- -D warnings` would close
   it.
9. **A repository-owned official gate block outranks generic pipeline worktree
   hygiene.** The Botster Stack Delivery step prompt tells a planner to set
   `CARGO_TARGET_DIR` on a colon-bearing worktree path, while this repository
   forbids setting it on official gates at all. The two rules are compatible only
   because the colon case is rare. A note stating that the repository rule wins,
   and that a colon path must be relocated instead, would remove the ambiguity
   before it produces a wrong gate run.

## 14. Revision record for this Plan visit, run `run_1787678814_340532`

The previous run `run_1787664777_379002` was cancelled without advancing feature
work, after the strict Rust gates were measured red on `main` and one
prerequisite ticket was registered to own both repairs. That prerequisite merged
as `a0c7141`. This visit restarts the plan from current `main`, exactly as the
answer to `question_1787667127_613797` instructed.

This visit did not re-derive the product plan. Sections 4 to 10 and 11.3 hold
unchanged, because the answered questions that produced them are still answered
and the underlying code did not move. What this visit changed:

| Change | Reason |
|---|---|
| Base commit `55f620d` → `a0c7141`; the branch was rebased onto `a0c7141` | the prerequisite merged |
| Section 1 dependency table: all three dependencies closed, no blocking dependency remains | `project_pipelines_current_context` reports `blocking_dependencies` empty |
| Section 11.2 rewritten: from "both strict gates red, owned by a prerequisite" to an independently re-measured green baseline | the prerequisite's report is not accepted on its own; both gates were re-run here |
| Section 11.1 gate block replaced with the repository's own official block | the prerequisite published it, together with `export RUSTUP_TOOLCHAIN=1.97.0` and `unset CARGO_TARGET_DIR` |
| Section 2 adds five gate-hygiene notes | all five were published by the prerequisite and none existed at the previous visit |
| Section 8 states the discovery command and the one historical `docs/reports/**` match | a fixed count is what made the note wrong in the first place |
| Section 12 risks: the "both gates red" row is replaced by four gate-execution rows | the inherited-failure risk is gone; the wrong-toolchain, `CARGO_TARGET_DIR`, and partial-clippy risks are the live ones |
| Section 13 marks gaps 1 and 8 partly closed and adds gap 9 | the charter moved between visits |
| `src/package_entity_fanout.rs` returns to the untouched-by-design list with no exception | its clippy repair is already on `main` |

Independent base re-verification performed by this visit, not carried forward
from the previous plan text:

1. `git fetch origin main`, `git rebase origin/main`. The branch carried only the
   two plan-document commits and rebased cleanly onto `a0c7141`.
2. `rustc --version` → `rustc 1.97.0 (2d8144b78 2026-07-07)`; `zig version` →
   `0.16.0`, both from the gate shell.
3. `cargo fmt --all -- --check` → exit `0`.
4. `cargo clippy --workspace --all-targets --locked -- -D warnings` → exit `0`,
   zero warning or error lines.
5. `git ls-remote` on `botster-core` → `358ef1a`, the roll target, unchanged.
6. The Core pin discovery grep → nineteen matches, eighteen active.
7. `packages/hub-test-support` version, `metadata.json` revisions, and the
   `botster-hub-client` constants → all unchanged.
8. Bare `npm test` → `ERR_MODULE_NOT_FOUND` before any assertion.
   `npm install --no-save && npm test` → exit `0`, then
   `git status --porcelain` empty.
9. Every symbol line number cited in sections 3 and 7 → re-measured at
   `a0c7141`. `src/local_webrtc.rs` is now 7871 lines; the four removed lines are
   all after `:7710`, so no cited reference moved.
10. `docs/plans/freeze-subscription-ownership-...md` §8.2 → still reads "Hub
    creates every subscription DataChannel", so the correction in scope item 10
    is still owed and has not been made by another ticket.

No blocking question is open for this run, and this visit asks none.

### 14.1 Plan Review return, review_1787680632_501854

Verdict `changes_required` with three findings. Each was re-measured against the
plan and the repository before it was accepted, per the role rule that a reported
failure needs exact evidence rather than agreement. All three are correct.

| Finding | Severity | Verdict after independent check | Resolution |
|---|---|---|---|
| `finding_1787680632_725415` — the open-timeout path tries to close a channel that does not exist | high, product | **Correct.** The late-message matrix row for a `Reserved` route that never opens read "Hub closes the channel, releases the slot, and emits `subscription_channel_open_timeout`". Admission creates no channel and the browser never created one, so there is no channel to close. This was a residue of the superseded architecture section 8.2 contract, carried into the corrected browser-created design | The matrix row now states that Hub closes nothing, retires the route, releases the slot, and emits the typed event. Section 6 adds an explicit statement of where bounded close does and does not apply. Row A8b now asserts that Hub issues **no** `local_close()` on this path |
| `finding_1787680632_224126` — acceptance does not prove mismatched identity or open-time over-limit rejection | high, product | **Correct.** The ticket requires validation of route identity, subscription identity, generation, and limits on every open event, and rejection of "mismatched" and "over-limit" opens. A8d opened only a label with **no** reservation, and A7 rejected the 33rd **admission** before any channel existed. Neither exercises a partial match against a live `Reserved` route, and neither exercises the open path with the table full | Two rows added. **A8f** opens one channel per mismatch axis of the section 8.3 label — wrong kind, wrong `session_id`, wrong `subscription_id`, wrong `generation` — each with every other field correct, and asserts the surviving `Reserved` route stays `Reserved` and bindable. A single all-fields-wrong label is rejected as a substitute, because it cannot distinguish a per-field comparison from a whole-string comparison. **A8g** fills the connection to `MAX_TOTAL_CHANNELS` = 33 and opens one extra channel, asserting `Open`, no adapter, unchanged accounting, and an undisturbed sibling proved by a delivered terminal marker |
| `finding_1787680632_446785` — the targeted-note inventory is incomplete | low, process | **Correct, and the count was wrong in both directions.** Section 2 listed 28 notes while the artifact and checklist evidence claimed 29. Six further notes are applied in section 11.3 and were not listed anywhere | Section 2 now carries a release-chain and downstream-proof group with all six notes, and states the corrected total of **34**. The artifact and gate evidence are corrected. No second checklist was created; `checklist_1787679234_157339` is reused, as the finding requires |

The reviewer's routing, charter, ownership, dependency, base, gate, and
runtime-teardown conclusions matched this plan. The reviewer independently
resolved `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub` through the
admitted spawn-target registry, confirmed base `a0c7141`, and reported that
format, strict Clippy, and the full locked wrapper pass under Rust `1.97.0` and
Zig `0.16.0`. That is an independent second measurement of the green baseline
recorded in section 11.2, and it also covers `./test.sh --locked`, which the Plan
step itself did not run.

Acceptance now carries **twenty** deterministic rows. Nothing in sections 1, 3 to
5, 7 to 10, or 11.3 changed.

### 14.2 Plan Review return, review_1787681114_793607

Verdict `changes_required` with one finding. The reviewer approved the
architecture, ownership split, registered dependencies, risks, assumptions,
runtime-teardown answers, live Hub proof, downstream package proof, the
open-timeout correction, A8f, and the corrected 34-note inventory. One row was
rejected.

`finding_1787681114_577880`, high, product — A8g did not make the open-time limit
guard load-bearing.

**Correct, and re-derived from the plan's own text before acceptance.** Section 4
item 3 and the section 6 production path both state that admission charges the
section 9 table and only then inserts a `Reserved` route. A `Reserved` route
therefore implies a successful charge. At a full connection the extra channel the
browser opens has no reservation, so the unreserved guard from A8d refuses it
first. Removing only the open-time limit predicate leaves A8g green. The row
proved the outcome, not the guard.

The plan already carried
`[[a regression test must be shown to go red with the fix reverted]]` and applied
it to A8. It was not applied to A8g. That is the actual defect: a known rule was
listed and then not used on a row that needed it.

The reviewer's suggested resolution is adopted in full, including its conditional
half. Production admission does make an isolated over-limit valid reservation
unreachable, so:

- New section 11.0 states why the check is defensive, and records that the
  previous revision's claim was false.
- **A8g** is rewritten to state its proof role explicitly — the externally
  visible rejection and sibling isolation only — and now asserts the typed
  **unreserved** reason, so it cannot silently reacquire the limit claim.
- **A8h** is added as a focused state-construction unit lane that drives the
  production open-event decision function against one `Reserved`,
  identity-matching, generation-matching route on a connection already at
  `MAX_SUBSCRIPTION_CHANNELS` = 32. Every other guard passes on that input by
  construction, so the limit predicate is the only check that can refuse it.
- A8h carries an **assertion-specific** red-on-revert: delete only the limit
  predicate, leave every other guard in place, and A8h must be the first failure.
- Both rows require **distinguishable typed rejection reasons**. A generic close
  would leave neither row able to name which guard refused the channel.

Acceptance now carries **twenty-one** deterministic rows. Section 12 gains one
risk row for the general failure mode: a guard proved by a test that an earlier
guard already satisfies is dead in practice, because it can be deleted with every
test still green.

### 14.3 Plan Review return, review_1787681637_806331

Verdict `changes_required` with two findings. The reviewer approved architecture,
ownership boundaries, registered dependencies, risks, assumptions,
runtime-teardown answers, live Hub proof, downstream package proof, the
open-timeout correction, A8f, the note inventory, and the two-lane proof concept
itself. Both findings are correct. Both are resolved.

`finding_1787681637_178709`, high, product — A8h treated the allowed count as
over-limit.

**Correct, and it was a real product defect, not a wording problem.** Section 9
charges the subscription slot when the route becomes `Reserved`, and
`MAX_SUBSCRIPTION_CHANNELS` = 32 **permits** 32 charged subscription routes.
Revision 3's A8h constructed a charged count of 32 and required a refusal. That
count is **at** the limit, not above it. Either the matching `Reserved` route was
included in the 32, in which case the route must bind and the row demanded the
wrong verdict, or it was excluded, in which case the constructed state silently
violated the charge invariant the same row relied on.

The consequence was concrete: an implementer following revision 3 would have
written the predicate as `>= 32` and rejected the valid 32nd subscription. The
row would have turned a defensive check into a live product defect. This is the
worst of the four findings across the three returns, because the previous three
weakened proofs while this one would have produced wrong behavior.

Resolution, adopting the suggested fix in full including its final clause:

- **A8h** now constructs 32 `Bound` routes plus one matching `Reserved` route,
  for 33 charged subscription routes, and **states that the count includes the
  matching `Reserved` route**. Its ablation removes only the
  greater-than-maximum predicate.
- **A8i** is added as the positive boundary control: 31 `Bound` plus one matching
  `Reserved`, for exactly 32 charged, which must **bind**. A8i fails if the
  predicate is written `>=` instead of `>`, which is precisely the defect an
  at-limit A8h would have taught.
- Section 11.0 states the boundary once, in one place: the slot is charged at
  `Reserved`, 32 charged routes are permitted, the predicate is `> 32` and never
  `>= 32`, and any constructed over-limit state must say whether its count
  includes the matching `Reserved` route.
- Section 12 gains a risk row for the general form: an over-limit acceptance row
  constructed at the limit instead of above it drives the implementation to
  reject the last valid item.

`finding_1787681637_408089`, low, process — A8g cited section 11.4 for an
explanation that lives in section 11.0.

**Correct.** Revision 3 wrote the explanation as section 11.0 and left the
cross-reference pointing at 11.4, which does not exist. Fixed. The reviewer
correctly classified this as process and said it does not need its own Plan loop.

Acceptance now carries **twenty-two** deterministic rows. Nothing else changed.
