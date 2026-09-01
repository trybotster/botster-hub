# Give entity and package-event subscriptions dedicated DataChannels

Ticket: `ticket_1787600682_233928`.
Run: `run_1788280061_893808`. Pipeline: `botster_stack_delivery`.

## 1. Target

| Field | Value |
|-------|-------|
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Repository path | `/Users/jasonconigliari/Projects/botster-hub` |
| Base ref | `main` |
| Base commit | `b4020a976010f4ec495c89efd6ea66271e02712f` ("Bound Unix ingress contention") |
| Base is `origin/main` | Yes. `git merge-base --is-ancestor HEAD origin/main` passes and `origin/main` resolves to the same SHA |
| Worktree | Clean. `git status --porcelain` is empty |
| Tracked `.gitignore` | Present, 53 bytes. No restore needed |
| Worktree path contains `:` | No. Do not set `CARGO_TARGET_DIR` |
| Locked Core revision | `e5a927c31d5b7d0b0f4b198e5e556ed75d53ddf1` for `botster-core`, `botster-core-daemon`, `botster-terminal-protocol`, `botster-core-test-support`, `botster-terminal-ghostty` |
| WebRTC crate | `webrtc = "0.21.0-beta.2"` |
| Rust toolchain | `1.97.0` (`rustc 1.97.0 (2d8144b78 2026-07-07)`), matching `.github/workflows/ci.yml` |
| Public protocol | `PROTOCOL_VERSION = 8`, `CONFORMANCE_FIXTURE_REVISION = 47`, `@trybotster/hub-test-support` `0.1.42` |

The repository was resolved from the ticket `target_id` through
`list_spawn_targets`. It was not inferred from the process working directory.

## 2. Repository playbook loaded

- `[[botster-hub-playbook]]` -- the exact ownership charter for `botster-hub`.

## 3. Other playbooks and atomic notes loaded

Role playbooks:

- `[[planner-playbook]]`
- `[[botster-planner-playbook]]`
- `[[botster-hub-client-playbook]]` -- loaded because this ticket adds a public
  `botster-hub-client` DTO.

Required Botster architecture context, from the `[[botster-planner-playbook]]`
Must Load list:

- `[[botster-architecture]]` -- the Botster domain map and source of architectural
  truth. It already records this project's channel decisions, so the plan is
  checked against it and not against the frozen artifact alone.
- `[[cli-patterns]]` -- Rust CLI, TUI, PTY, and terminal-layer constraints.
- `[[spa-patterns]]` -- React and entity-store frontend constraints, which bound
  what the Web consumer ticket can rely on.
- `[[botster orchestration should spawn agents with explicit target ids]]`
- `[[botster orchestration prompts must bind agents to explicit worktrees]]`
- `[[current botster is a modular repository family not the legacy trybotster monorepo]]`
- `[[botster hub is a first party host profile over core]]`
- `[[botster Hub Rust stays a trusted host kernel]]`
- `[[lua plugins are the hub composition layer]]`
- `[[concrete terminal transports stay in hub until a second host needs them]]`

The three Project Pipelines notes in that Must Load list
(`[[project pipeline orchestration belongs in a device-level botster plugin]]`,
`[[project pipelines needs an operator workbench not more primitives]]`,
`[[project pipelines ui contract belongs in the plugin readme]]`) were read and
found not applicable: this ticket changes no Project Pipelines package, plugin, or
workflow policy.

Downstream DTO consumer proof:

- `[[tui shaped Hub consumer proofs must include hub test support]]`
- `[[a ui contract import line change costs one test line in each generic client]]`
- `[[clean consumer smokes resolve exported root entrypoints not package json]]`
- `[[Hub test support capability cutovers use a new unpublished package version]]`
- `[[closed dependency tickets signal merged source not a consumable release]]`
- `[[registry integrity compared against a pack of the intended commit retires stale tree publish risk]]`

Class overlay:

- `[[botster runtime teardown lenses]]` -- the runtime-teardown class applies.
  Section 12 answers every required field.

Transport and channel contract:

- `[[botster subscriptions use dedicated ordered DataChannels]]`
- `[[the browser creates each subscription DataChannel after Hub reserves its label]]`
- `[[webrtc 0 21 restores post handshake DataChannel creation in Hub]]`
- `[[rejected channel isolation needs a surviving channel positive control]]`
- `[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`
- `[[a ready WebRTC send must win over a queued DataChannel close]]`
- `[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`
- `[[PeerClosed attach occupancy must use the live attach route set]]`
- `[[core owns duplex terminal transport while Hub stays content blind]]`
- `[[botster data plane bypasses the hub through session and client actors]]`

Entity and package-event contract:

- `[[Client event subscriptions stay on the multiplexed host-control path]]`
- `[[Fair host-control writing selects already-admitted frames]]`
- `[[Client event holders are connection-scoped]]`
- `[[exact owner plus name is the only package event subscription key]]`
- `[[Package-event subject filters are exact strings compiled at admission]]`
- `[[admitted event holders survive producer unload until Core completion]]`
- `[[botster hub events use bounded priority lanes instead of unbounded queue fuses]]`
- `[[saturation counters do not acquire the contended lock they report]]`

Move, guard, and gate discipline:

- `[[code moves need paired absence and presence source guards]]`
- `[[hub moves must extend source scanning guard file lists]]`
- `[[fixed source guard lists need one ablation per added file]]`
- `[[region bounded source guards need a required symbol anchor]]`
- `[[source guard ablations must not overlap a running full suite]]`
- `[[exact Rust test ablations require a one test baseline]]`
- `[[a regression test must be shown to go red with the fix reverted]]`
- `[[rust file splits can silently widen private helper visibility]]`
- `[[Hub official gates must not set CARGO TARGET DIR]]`
- `[[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]`
- `[[strict clippy can hide later crate diagnostics behind the first compile failure]]`
- `[[Hub test support version bumps must update the Node mirror test literals]]`
- `[[vault example paths are not repository placement conventions]]`

`[[project-pipelines-playbook]]` is not loaded. No Project Pipelines package or
plugin path is in scope.

Every note title above was checked against its exact vault filename before this
plan was submitted.

## 4. Context loaded

- Ticket `ticket_1787600682_233928` and project `project_1787600579_585482`,
  including the project's frozen ownership, frozen WebRTC topology, decomposition
  order, and direct-cut revision of 2026-08-29.
- `artifact_1787606577_290231`, the frozen architecture contract. Its final merged
  form is in this repository at
  `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md`.
  Sections 8, 9, 11, 12, 14.3, and the acceptance matrix in section 14 are binding
  here. The artifact identity was confirmed through
  `project_pipelines_current_context` for run `run_1787605830_934897`.
- `docs/plans/cold-cut-wake-driven-duplex-terminal-transports.md`, the merged
  predecessor. Its non-scope list names "Entity and package-event dedicated
  DataChannels".
- Merged Hub source: `src/transport/webrtc/*`, `src/transport/unix/mux_write.rs`,
  `src/admission/*`, `src/subscription/*`, `src/daemon/control/*`,
  `src/host_control_fair_write.rs`, `src/config.rs`, `src/lib.rs` source guards,
  and `crates/botster-hub-client/src/lib.rs`.
- `.github/workflows/ci.yml` and `test.sh`.
- Dependencies, both closed: `ticket_1787600674_500120` (superseded) and
  `ticket_1787894427_525056` (the merged cold cut).
- Open sibling tickets checked. `ticket_1787600684_892051` (Web consumer),
  `ticket_1787600676_914408` (Web terminal), `ticket_1787600679_990088`
  (Integration), `ticket_1787600691_401181` (Hub kernel boundary),
  `ticket_1788206393_323469` (Hub resize reproduction), and
  `ticket_1787603674_865638` (TUI) are open. Only `ticket_1788206393_323469`
  targets `botster-hub`, and it touches `pump_woken` resize, not transport channel
  topology.

## 5. Human decisions that shape this plan

Three ticket clauses did not match the merged repository. All three were resolved
by the human before this plan was written. The answers are binding.

### 5.1 `question_1788280460_912491`, answer Q1 Option A

The "existing connection limit table" does not exist in Rust source. Section 9 of
the frozen artifact was assigned to `ticket_1787600674_500120`, which closed as
superseded, and the cold cut declared this work non-scope.

Decision: this ticket creates the single section 9 connection budget table and one
accounting owner in Hub admission, covering control, terminal, entity, and
package-event channels. The table applies at reservation, bind, rejection,
timeout, replacement, close, and retirement. Every failure path must prove no
count drift and no byte drift. Transport modules report accepted configuration and
usage; transport modules do not own budget policy. No separate aggregate-limit
ticket is created.

### 5.2 `question_1788280460_912491`, answer Q2 Option A with an ownership cleanup

Decision: remove the WebRTC fair-write call site, its Entity and Event arms, the
now-dead `HostControlClass::Entity` variant, and the `entity_ready` parameter.
Keep the Control and Event rotation for Unix, because Unix stays multiplexed. Move
the remaining scheduler out of `src/host_control_fair_write.rs` into
`src/transport/unix/` as Unix-owned code. Do not move Unix package events in this
ticket. Record artifact section 12.3's whole-file behavior deletion as **deferred**
because its stated repository-wide condition is false. Do not claim the scheduler
behavior is gone while Unix still uses it.

### 5.3 `question_1788280530_470242`, answer Option C

Decision: create one mailbox per package-event subscription with the current
per-subscription maxima of 128 events and 2 MiB. Also preserve the current
per-connection residency ceilings of 128 events and 2 MiB across all package-event
mailboxes on one connection. The WebRTC `bufferedAmount` ceiling does not bound
queued mailbox memory. The aggregate policy must preserve isolation:

- Charge each frame to exactly one subscription and one connection.
- Never evict, reorder, gap, or close a sibling when another subscription exceeds
  its share.
- Apply an aggregate rejection or gap only to the subscription that requested the
  unavailable capacity.
- Use fair admission or reserved shares so one noisy subscription cannot consume
  every sibling's usable capacity.
- Release both charges exactly once on send, drop, gap recovery, close, timeout,
  and generation retirement.

64 MiB per peer is not accepted as residual risk.

### 5.4 `question_1788281660_218527`, answer Option B

Raised after Plan Review `finding_1788281488_295603` showed my `floor(cap / live_N)`
share did not protect a later sibling.

Decision: use an elastic connection pool with a fixed reserve of 4 events and
65,536 bytes for each admitted package-event subscription. A subscription may
borrow only capacity that is not reserved for another admitted subscription.
Before admitting a new subscription, require its full event reserve and byte
reserve to be free; if either is unavailable, reject `SubscribeEvents` immediately
with one typed capacity reason. Do not evict, gap, close, or shrink an existing
subscription. Do not add a waiting admission queue; the client retries after
capacity drains. After admission the reservation is generation-scoped and released
exactly once on rejection rollback, timeout, close, replacement, and retirement.

Five proof cases were named and are acceptance check 9 arms (a) through (e).

## 5.5 Plan Review response

`review_1788281488_310273` returned `changes_required` with six findings. This
revision answers all six.

| Finding | Severity | Fix |
|---------|----------|-----|
| `finding_1788281488_295603` -- the live-N mailbox share does not protect a later sibling | high | Accepted; the defect was real. Redesigned in 5.4 and 6.5 as an elastic pool with a fixed reserve and fair admission rejection. Acceptance check 9 now has six arms and a red-on-revert control that restores the broken share and asserts arm (d) fails first |
| `finding_1788281488_909963` -- the plan leaves required terminal aggregate behavior unowned | high | Accepted. A26, A27, and A27b moved from non-scope into scope as section 6.10 and acceptance checks 15, 16, and 17. `WRITE_ATTEMPT_BUDGET` re-verified as 512 at the current pin `e5a927c`, at `client_worker.rs:34` rather than the artifact's cited line 30 |
| `finding_1788281488_393109` -- the public DTO change lacks required downstream source proof | high | Accepted. Acceptance check 12 now separates serde wire proof from downstream source proof, requires a TUI-shaped consumer worktree through a local Cargo patch redirect, records TUI and Web client costs, and adds an exported-root clean-install smoke |
| `finding_1788281488_533842` -- the plan omits an open same-target publication conflict | high | Accepted. Section 8.1 records all five open `botster-hub` siblings, names the `hub-test-support` 0.1.42 publication conflict, keeps 0.1.42 unmutated, and registers `ticket_1788280618_295967` as a blocking dependency ahead of the 0.1.43 bump |
| `finding_1788281488_593455` -- required Botster architecture context is absent from the recorded note list | medium | Accepted. Section 3 now records the `[[botster-planner-playbook]]` Must Load set, including `[[botster-architecture]]`, `[[cli-patterns]]`, and `[[spa-patterns]]`, and states why the three Project Pipelines notes do not apply |
| `finding_1788281488_917063` -- Plan gate evidence omitted required fields | low | Partly a visibility defect, not a missing submission. `gate_result_1788281113_199176` carried all twelve required fields and passed, but the `step.completed` event for `run_step_1788280061_415917` recorded `evidence: {}` because `request_step_advance` was called without an evidence argument. This revision passes the evidence object to `request_step_advance` as well as `submit_gate`, and states the checklist id inline |

## 6. Scope

### 6.1 Connection budget, one table and one owner

New module `src/admission/connection_budget.rs`, owned by Hub admission.

- `ChannelClass { Control, Terminal, Entity, Event }`.
- `MAX_CONTROL_CHANNELS = 1`, `MAX_SUBSCRIPTION_CHANNELS = 32`,
  `MAX_TOTAL_CHANNELS = 33`, `AGGREGATE_BUFFERED_HIGH = 2_097_152`,
  `AGGREGATE_BUFFERED_LOW = 1_048_576`.
- One `ConnectionBudget` per peer generation, stored in
  `PendingRuntimeState::admission` beside the reservation registry.
- Count is charged when a route becomes `Reserved` and released exactly once on
  bind failure, open timeout, replacement, close, peer loss, and retirement.
- `aggregate_buffered()` is a derived sum over `Bound` subscription channels, per
  artifact section 9.2. There is no stored running total and no decrement on
  `bufferedAmountLow`. Transport reports each channel's current `bufferedAmount`
  into a usage registry that admission reads; admission owns the predicates.
- The two predicates stay distinct, exactly as artifact section 9.3 states:
  reject admission when `aggregate_buffered() >= AGGREGATE_BUFFERED_HIGH`; refuse
  a send when `aggregate_buffered() + frame_len > AGGREGATE_BUFFERED_HIGH`.
- The control channel is outside the aggregate, per artifact section 9.1. That
  exclusion is what keeps the overflow report sendable.
- Per-channel `bufferedAmount` thresholds keep their shipped values
  (`LOCAL_WEBRTC_BUFFERED_AMOUNT_LOW` 64 KiB, `..._HIGH` 128 KiB). Their scope is
  already per channel in `run_bound_subscription_channel`.

There is one accounting site. No second limit path is added.

### 6.2 Reservation for entity and package-event routes

`src/admission/reservations.rs` generalizes from terminal-only to class-aware.

- The registry gains `ChannelClass` in its key. Terminal keeps its shipped key
  `(session_id, subscription_id, generation, peer_generation)`. Entity and
  package-event routes key on `(class, subscription_id, generation, peer_generation)`
  with `grant_id` implied by the peer, per artifact section 8.3's ownership rule
  for `ent` and `evt`.
- `generation` for entity and package-event routes comes from a per-grant
  monotonic counter, added beside the existing `next_peer_generation` in
  `src/admission/peer_generation.rs`.
- Labels stay opaque `r-<hex>` values minted by `unique_label`. See assumption
  9.2 for why the artifact's structured `bs/1/...` label scheme is not used.
- `reserve`, `lookup_label`, `reservation_for_label`, `expire_label`,
  `mark_bound`, `retire_expired`, `forget_route`, and `forget_peer` all become
  class-aware. `forget_peer` stops being `#[allow(dead_code)]`: peer loss must now
  retire entity and package-event reservations and release their budget slots.
- Reservation expiry keeps `TERMINAL_RESERVATION_EXPIRES_IN_SECONDS = 30` and the
  `BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS` test override.

### 6.3 Admission responses return label and generation

`crates/botster-hub-client`:

- Add `DaemonSubscriptionReservation { kind, subscription_id, generation,
  peer_generation, label, expires_in_seconds }`, `#[non_exhaustive]`, with
  `kind: DaemonSubscriptionReservationKind { Entity, PackageEvent }`.
- Add `DaemonResponse::subscription_reservation: Option<DaemonSubscriptionReservation>`
  with `#[serde(default, skip_serializing_if = "Option::is_none")]`.
- Generate the matching TypeScript in `crates/botster-hub-client/src/typescript.rs`.
- Leave `DaemonTerminalReservation` and the `terminal_reservation` field
  unchanged, so the shipped terminal contract and the open Web terminal ticket
  `ticket_1787600676_914408` are not disturbed.

This is a public Rust DTO field addition, so `[[botster-hub-client-playbook]]`
requires serde wire proof and downstream **source** proof to stay separate, and
requires representative consumer worktrees to build through a local Cargo patch
redirect. Section 13 check 12 names both. The npm coordinate is constrained by an
open publication ticket; see section 8.1.

Hub returns the reservation on the `EntitySubscribed` and `EventSubscribed`
responses over the control channel, immediately, without waiting for channel
`open`.

### 6.4 Entity transport moves to dedicated channels (artifact R6)

- One bounded `tokio::sync::mpsc` channel per admitted entity subscription, with
  capacity `ENTITY_SUBSCRIPTION_QUEUE_CAPACITY` = 64. Today one channel of the
  same capacity is shared by every entity subscription on a peer, because
  `entity_frame_tx` is cloned per `SubscribeEntities`.
- `EntityFrameSender::Async` is constructed per subscription at admission. The
  receiver moves into that subscription's channel host on bind.
- Remove from `src/transport/webrtc/control_channel.rs`: the `entity_frame_tx` and
  `entity_frame_rx` parameters, `pending_entity`, the `HostControlClass::Entity`
  arm, and the `owns_entity_subscription` post-hoc filter. Ownership is now the
  reservation itself.
- `framed_daemon_entity_frame` and `entity_frame_subscription_id` move to the
  subscription channel host.
- Entity frame **authoring** stays in `src/subscription/entity.rs` and
  `src/package_entity_fanout.rs`, unchanged. Only the transport binding moves.
  Entity subscription scope and pull-owned hydration rules, including the existing
  `subscriber_overflow` resync path, are preserved. See assumption 9.3.

### 6.5 Package-event transport moves to dedicated channels (artifact R7)

- `ClientEventPlane` keys mailboxes by `(connection_id, subscription_id)` instead
  of `connection_id`.
- Each mailbox keeps `consumer_queue_max_events` = 128 and
  `consumer_queue_max_bytes` = 2 MiB.
- A new per-connection residency ceiling of 128 events and 2 MiB spans all
  package-event mailboxes on one connection.
- **Elastic pool with a fixed reserve and fair admission.** An earlier draft used
  a share of `floor(cap / live_N)` recomputed at push time. Plan Review correctly
  rejected it (`finding_1788281488_295603`): a subscription admitted alone can
  fill all 128 events, and a later sibling's share is then already occupied, so
  the guarantee never covered later siblings. Human decision
  `question_1788281660_218527` Option B replaces it:
  - Each admitted package-event subscription holds a **fixed reserve** of 4 events
    and 65,536 bytes.
  - A subscription may **borrow** only capacity that is not reserved for another
    admitted subscription. One subscription alone can therefore still use the full
    128-event and 2 MiB depth.
  - Before admitting a new subscription, both its event reserve and its byte
    reserve must be free. If either is unavailable, `SubscribeEvents` is rejected
    immediately with one typed capacity reason.
  - No existing subscription is evicted, gapped, closed, or shrunk to make room.
  - There is **no waiting admission queue**. The client retries after capacity
    drains.
  - The reservation is generation-scoped and released exactly once on rejection
    rollback, timeout, close, replacement, and retirement.
- A push refusal sets the gap bit for the requesting subscription only, using the
  shipped `event_gap` policy. No sibling is evicted, reordered, gapped, or closed.
- Both the subscription charge and the connection charge are released exactly once
  on send, drop, gap recovery, close, timeout, and generation retirement.
- Every Hub-owned event bound in `src/config.rs:345-361` -- payload, fanout, rate,
  burst, queue age, producer queue, global in-flight -- keeps its current value and
  meaning.
- Package-event subscription scope is unchanged: exact `(owner, name)` identity per
  `[[exact owner plus name is the only package event subscription key]]`, exact
  compiled subject sets per
  `[[Package-event subject filters are exact strings compiled at admission]]`,
  connection-scoped holders per `[[Client event holders are connection-scoped]]`,
  and admitted-job survival per
  `[[admitted event holders survive producer unload until Core completion]]`.

### 6.6 Bind, rejection, and retirement on channel open

The shipped `admit_reserved_subscription_channel` in
`src/transport/webrtc/subscription_channel.rs` is extended, not duplicated. On a
browser-created channel open:

1. Inspect the reservation for `(grant_id, label)` at the peer generation.
2. Reject `Unknown` (unreserved or mismatched), `Bound` (duplicate), and
   `Expired` (late or stale) through `reject_extra_data_channel`, which closes
   under `LOCAL_WEBRTC_PEER_CLOSE_BOUND`.
3. Reject over-limit opens: if the budget cannot account for the class, close and
   release. Over-limit is normally caught at reservation, so an over-limit open
   is a fail-closed backstop.
4. Require the encrypted `DaemonHello` admission before bind, unchanged.
5. Bind the route by class: terminal binds the Core adapter as today; entity binds
   its per-subscription frame receiver; package event binds its per-subscription
   mailbox.
6. Retire the reservation and release both budget charges on close, replacement,
   cancellation, peer loss, and open timeout.

The browser creates every subscription channel. Hub reserves and admits. This
matches `[[the browser creates each subscription DataChannel after Hub reserves its label]]`,
the project's frozen WebRTC topology, and the shipped terminal implementation.

### 6.7 Entity overflow close, artifact rows E1 through E4

When the aggregate predicate refuses an entity frame send, the production handler
executes in this exact order and records a handler-boundary trace:

| Step | Action |
|------|--------|
| E1 | The send refusal is decided before any transport write |
| E2 | Hub admits the typed `entity_subscription_overflow` reason to the control channel's send path |
| E3 | Hub calls `local_close()` on that entity subscription channel under `LOCAL_WEBRTC_PEER_CLOSE_BOUND` |
| E4 | The route moves to `Retired`, its budget charges are released, and its buffered bytes leave `aggregate_buffered()` |

E2 is admission to the send path, not remote receipt. Close never waits for remote
delivery.

### 6.8 Fair-write removal and Unix ownership move (artifact R8, D7, 12.3)

- Delete `flush_ready_webrtc_host_control` from
  `src/transport/webrtc/control_channel.rs`, together with its Entity and Event
  arms, `host_event_ready`, `take_host_event`, and the dead
  `flush_webrtc_host_events`.
- Replace it with a plain single-class control writer. Hub commands, responses,
  and small lifecycle events all travel on the Hub control DataChannel as one
  FIFO class. Hub lifecycle close events from `mux.pop_pending_event()` are
  enqueued into that same control writer, keeping the existing
  `close_events_admitted()` negotiation gate. See risk 11.4.
- Delete the `HostControlClass::Entity` variant and the `entity_ready` parameter,
  which no caller sets after this change.
- Move the remaining Control and Event rotation out of
  `src/host_control_fair_write.rs` into `src/transport/unix/host_write_order.rs`
  as Unix-owned code, and delete `src/host_control_fair_write.rs`. Update the
  `src/lib.rs` production-scan file list for the new path and remove the old one.
- Record artifact acceptance row A9 as **partially met**: the file
  `src/host_control_fair_write.rs` is gone and returns zero grep matches, but the
  scheduler *behavior* is deferred, not deleted, because Unix still rotates
  Control and Event on one framed socket.

### 6.9 Non-scope

- `botster-web` client work. `ticket_1787600684_892051` consumes this merge.
- `botster-tui` client work.
- Moving Unix package events or Unix entity subscriptions off the multiplexed
  socket. Artifact D6 keeps the Unix entity stream out of project scope.
- Per-subscription SDP renegotiation. Explicitly forbidden by the ticket.
- A pre-created channel pool. Explicitly forbidden by the ticket.
- Cross-channel delivery ordering. Hub must not add it.
- Changing the label scheme, encryption derivation, or chunking of the shipped
  terminal subscription channel.
- Rolling the Core pin. It stays at `e5a927c3`.
Artifact rows A26, A27, and A27b were previously listed here as non-scope. That
was wrong and Plan Review rejected it (`finding_1788281488_909963`). This ticket
puts the aggregate ceiling into the terminal write path, so it owns the terminal
consequences of its own change. They are now in scope as section 6.10 and
acceptance checks 15, 16, and 17.

### 6.10 Terminal consequences of the aggregate, in scope

The human decision in 5.1 covers the terminal class explicitly. Putting the
aggregate predicate in the terminal write path changes terminal behavior, so this
ticket owns and proves that behavior instead of leaving it to a superseded ticket.

- A refused terminal send is **backpressure, not loss**. `try_write` returns
  `TerminalAdapterWriteError::WouldBlock`, not `Full`, because the single
  active-write slot is empty while the transport is not ready. `pressure()`
  reports `TerminalAdapterPressure::WouldBlock`. Core retains the frame and the
  Hub adapter must not retain it. Hub queues nothing, drops nothing, reorders
  nothing, and retries nothing.
- The aggregate is evaluated only on an **attempted** send. Saturation on its own
  never calls the terminal adapter, so a test that asserts terminal pressure
  without offering a frame exercises nothing.
- The aggregate does not drift: after a full drain, `aggregate_buffered()` returns
  to 0 and held classes resume. This is the property the derived sum of artifact
  section 9.2 has and a stored counter does not.
- **Sustained saturation converts to terminal route teardown, and this plan
  accepts that deliberately.** At the current Core pin `e5a927c`,
  `crates/botster-core/src/engine/client_worker.rs:34` sets
  `WRITE_ATTEMPT_BUDGET = 512`. Every `WouldBlock` or `Full` result increments
  `unsuccessful_writes`; a successful write resets it, so the budget counts
  consecutive failures. At the budget Core calls `hard_stop` and tears the route
  down. Core retention is therefore bounded, and no claim of unbounded retention
  is made anywhere in this plan. The artifact cited this as `7eafa47:30`; the
  value is still 512 at the current pin but the line is now 34. Verified by
  reading the pinned crate source, not by trusting the artifact.
- Mitigations: keep `AGGREGATE_BUFFERED_LOW` reachable quickly, and keep control
  outside the aggregate (artifact section 9.1) so the teardown notice stays
  sendable.

## 7. Repository ownership boundaries

- **`botster-core`** owns terminal subscription identity, generations, attach
  phases, duplex bytes, ordering, bounded queues, pressure, fencing, teardown,
  wakes, and targeted pumping. This ticket adds no terminal semantics and changes
  no Core pin.
- **`botster-hub`** owns admission, grants, labels, reservations, peer
  generations, the section 9 connection budget, route state, and the concrete
  Unix and WebRTC transports. All work in this ticket sits here.
- **`botster-hub-client`** owns the external DTO boundary. It is an in-repository
  workspace crate at `crates/botster-hub-client`, so its change merges with this
  ticket rather than crossing a repository seam.
- **Entity and package-event frames are Hub-authored and Hub-bounded.**
  Content-blindness applies to terminal channels only, per artifact section 10.
- **Lua plugins** stay outside terminal and transport hot paths.

## 8. Cross-repository dependencies

- `ticket_1787600674_500120` -- closed as superseded. It produced no code. Its
  section 9 obligations transfer to this ticket by the human decision in 5.1.
- `ticket_1787894427_525056` -- closed and merged. This ticket builds on its
  reservation registry, subscription channel host, and per-channel pressure.
- No new cross-repository prerequisite exists. One **same-repository** blocking
  dependency is registered; see section 8.1.

### 8.1 Open same-target siblings, including a publication conflict

The first sibling scan in this run was taken at about epoch 1788280300 and found
one open `botster-hub` ticket. Two more were created during this Plan step. Plan
Review was right that the plan omitted them (`finding_1788281488_533842`). A
rescan through `search_tickets(target_id, status=open)` gives the current set.

| Ticket | Title | Interaction |
|--------|-------|-------------|
| `ticket_1788280618_295967` | Hub: publish `@trybotster/hub-test-support` 0.1.42 with the terminal reservation DTO | **Blocking publication conflict.** Registered as a dependency |
| `ticket_1788280452_111197` | Hub: move bound-adapter test progress off Core drain onto wake pumps | Semantic-rebase risk. It rewrites about 66 test call sites, including `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` and `src/subscription/attach_routes.rs`, which this ticket also changes. Renew review after any semantic rebase |
| `ticket_1788206393_323469` | Hub: reproduce targeted `pump_woken` resize with merged Core | Low. It rolls the Core pin and resize behavior, not channel topology |
| `ticket_1787600679_990088` | Integration: cold-cut the old terminal route and prove isolation | Downstream consumer of this merge, not a prerequisite |
| `ticket_1787600691_401181` | Hub: enforce the Rust kernel and Lua composition boundary | Downstream audit of this merge, not a prerequisite |

**The publication conflict.** `main` already carries
`packages/hub-test-support` at 0.1.42 with `PROTOCOL_VERSION` 8,
`CONFORMANCE_FIXTURE_REVISION` 47, and `DaemonTerminalReservation`, but npm
publishes at most 0.1.41. `ticket_1788280618_295967` exists to publish 0.1.42, and
`botster-web` `ticket_1787600676_914408` stays blocked until it does.

An earlier draft of this plan bumped the repository straight to 0.1.43. That would
have stranded 0.1.42 as a version that never matches a published artifact, and it
would have broken the premise of `ticket_1788280618_295967`, whose acceptance
requires the published `daemon-protocol.ts` and metadata sha256 values to equal
the repository artifacts at 0.1.42.

Resolution, in order:

1. Do **not** mutate `packages/hub-test-support` at 0.1.42. Per
   `[[Hub test support capability cutovers use a new unpublished package version]]`
   a new capability takes a new version, and 0.1.42 additionally carries a pending
   publish coordinate that another ticket and a Web consumer depend on.
2. `ticket_1788280618_295967` is registered as a **blocking dependency** of this
   ticket, so 0.1.42 publishes before this ticket bumps to 0.1.43.
3. Then bump to 0.1.43 with `CONFORMANCE_FIXTURE_REVISION` 48 and the
   `subscription_reservation` DTO.
4. Per `[[closed dependency tickets signal merged source not a consumable release]]`,
   the workspace-internal Rust change needs only a merged `origin/main` commit,
   while the npm coordinate needs an actual publish. Those are different
   availability proofs and this plan does not conflate them.
5. Independently re-verify the dependency's output instead of trusting this plan's
   text: read the published 0.1.42 metadata, and compare registry integrity
   against a pack of the intended commit per
   `[[registry integrity compared against a pack of the intended commit retires stale tree publish risk]]`.
- Downstream consumers of this merge, already registered as separate tickets:
  `ticket_1787600684_892051` (Web entity and event channels) and
  `ticket_1787600679_990088` (Integration cold cut). They are consumers, not
  prerequisites, so this run does not broaden into them.

## 9. Assumptions and unknowns

1. **The browser creates every subscription channel.** Artifact section 8.2 says
   Hub creates them. That text is superseded by
   `[[the browser creates each subscription DataChannel after Hub reserves its label]]`
   (2026-08-25, later than the artifact), by the project's frozen WebRTC topology,
   by this ticket's own wording, and by the merged terminal implementation. The
   artifact's section 9 limits, section 11 bounds, and section 14.3 acceptance
   rows remain binding. Recorded as a convention conflict, resolved toward the
   later decision.
2. **Labels stay opaque.** Artifact section 8.3 specifies a structured
   `bs/1/<kind>/...` label. The merged code mints opaque `r-<hex>` labels and
   documents "Labels are opaque. Hub never derives peer-visible meaning from their
   contents." Opaque labels are kept, because a structured label would leak
   session and subscription identity to the peer and would fork the shipped
   terminal contract. Recorded as a convention conflict, resolved toward shipped
   code.
3. **Two different entity overflows exist and only one changes.** The shipped
   `EntityFrameTrySendError::Full` path sets `resync_reason = "subscriber_overflow"`
   and re-hydrates. Artifact rows E1 through E4 describe the transport aggregate
   refusal, which E1 defines as "the send refusal is decided, before any transport
   write". This plan adds the transport refusal close and leaves the mpsc queue
   resync path unchanged, because the ticket requires preserving "pull-owned
   hydration rules". The artifact's per-channel `ent` overflow row is ambiguous
   between the two; this reading is recorded for Plan Review to confirm or reject.
4. **`peer_generation` is the correct owner tag for entity and package-event
   routes.** It is minted per admitted WebRTC grant in `register_webrtc_admission`
   and already fences the terminal reservation registry against a reused
   `grant_id`.
5. **`MAX_SUBSCRIPTION_CHANNELS` = 32 is a per-peer ceiling, and
   `MAX_SUBSCRIPTIONS_PER_CONNECTION` = 64 in `src/subscription/package_events.rs`
   stays unchanged.** The two bounds now interact: a connection can hold 64
   admitted package-event subscriptions but only 32 subscription channels of all
   classes. Reservation therefore fails on the channel budget before the
   subscription budget. This is intended fail-closed behavior and is asserted, not
   silently tolerated.
6. Unknown, to be resolved in Implement: whether reading each channel's
   `bufferedAmount` for the derived aggregate is available synchronously on the
   `webrtc 0.21.0-beta.2` handle from the admission owner thread, or whether
   transport must publish it into an atomic usage cell. The plan requires the
   atomic-cell form if the synchronous read would block the owner thread, per
   `[[saturation counters do not acquire the contended lock they report]]`.
7. Unknown, to be resolved in Implement: the exact `hub-test-support` fixture
   surface that must carry `subscription_reservation`. Section 10 lists the sites
   known today.

## 10. Affected surfaces and files

New:

- `src/admission/connection_budget.rs` -- the single section 9 table and its
  accounting owner.
- `src/transport/unix/host_write_order.rs` -- the Unix-owned Control and Event
  rotation moved out of `src/host_control_fair_write.rs`.

Deleted:

- `src/host_control_fair_write.rs` -- moved, not retained. No forwarding wrapper.

Changed:

- `src/admission/reservations.rs` -- class-aware registry, per-grant subscription
  generations, live `forget_peer`.
- `src/admission/peer_generation.rs` -- per-grant subscription generation counter.
- `src/admission/budgets.rs` -- re-export or reference the new table without
  duplicating values.
- `src/admission/unix_hello.rs` -- `WebrtcTerminalAdmission` carries the budget.
- `src/daemon/control/connection.rs` -- inspect and bind for all three classes,
  budget charge and release on every path.
- `src/daemon/control/message.rs` -- class-aware `InspectTerminalReservation`,
  `BindReservedTerminal`, and `ReservationInspectReply`.
- `src/daemon/control/entities.rs`, `src/daemon/control/events.rs` -- reserve at
  admission and return the reservation.
- `src/subscription/entity.rs` -- per-subscription sender construction.
- `src/subscription/package_events.rs` -- per-subscription mailboxes, the
  per-connection residency ceiling, and fair shares.
- `src/transport/webrtc/control_channel.rs` -- plain single-class control writer;
  entity and event arms removed.
- `src/transport/webrtc/subscription_channel.rs` -- entity and package-event
  channel hosts, class-aware admission and bind, entity overflow close.
- `src/transport/webrtc/peer.rs` -- remove the shared entity channel; peer cleanup
  retires all classes.
- `src/transport/webrtc/delivery.rs` -- framing moves for entity and event frames.
- `src/transport/unix/mux_write.rs` -- import the moved scheduler.
- `src/lib.rs` -- production-scan file list: add
  `src/admission/connection_budget.rs` and `src/transport/unix/host_write_order.rs`,
  remove `src/host_control_fair_write.rs`.
- `crates/botster-hub-client/src/lib.rs` and `src/typescript.rs` -- the new DTO,
  `CONFORMANCE_FIXTURE_REVISION` 47 to 48.
- `packages/hub-test-support/package.json` 0.1.42 to 0.1.43, only after
  `ticket_1788280618_295967` publishes 0.1.42 (see section 8.1);
  `packages/hub-test-support/README.md`, and every version, protocol, and revision
  literal in `packages/hub-test-support/test.mjs` (lines 135, 138, 178, 383, 421,
  and 738 today).
- `crates/botster-hub-test-support/src/lib.rs` -- fixture updates.
- `docs/client-protocol.md` -- the new response field.

## 11. Risks

1. **The budget is a new cross-cutting policy owner.** It is charged on eight
   distinct transitions. A missed release leaks a channel slot and eventually
   refuses every new subscription on that peer. Mitigation: one accounting site,
   and a drift assertion on every failure path (acceptance check 6).
2. **The derived aggregate reads transport state from admission.** A blocking read
   on the owner thread would be a control-plane stall. Mitigation: assumption 9.6
   requires the atomic usage cell if the direct read can block.
3. **Fair admission can reject a subscribe that would have succeeded before this
   change**, when a noisy sibling holds borrowed capacity. This is a deliberate,
   visible, typed failure chosen over silent starvation, and there is no waiting
   admission queue, so the client must retry. Mitigation: acceptance check 9 arms
   (b) and (c) assert the rejection is typed, that no existing subscription is
   evicted or shrunk, and that admission succeeds once capacity drains.
4. **Folding lifecycle close events into the single control writer changes their
   ordering relative to responses.** Today the fair writer rotates between them.
   After the change they share one FIFO. A continuous request stream must not
   starve a pending close event. Mitigation: acceptance check 10 asserts a pending
   lifecycle close event still reaches the client under continuous control
   traffic, with a bounded number of frames per turn.
5. **A9 cannot be met as written.** The file is deleted but the behavior is
   deferred, per human decision 5.2. Recorded, not hidden.
6. **Semantic rebase.** Four other tickets are open against `botster-hub`; see
   section 8.1. `ticket_1788280452_111197` is the sharpest risk, because it
   rewrites about 66 test call sites in files this ticket also changes. Renew
   review after any semantic rebase, per the project's delivery rules.
7. **Test count and runtime.** This ticket adds a 31-channel saturation harness.
   `[[source guard ablations must not overlap a running full suite]]` applies:
   complete and restore every source mutation before starting the official locked
   gate. Nested `cargo test` children recompile from the live tree, so a mid-suite
   source edit invalidates the run.
8. **The aggregate ceiling converts sustained saturation into terminal route
   teardown** after 512 consecutive unsuccessful attempts. This is a real behavior
   change, not a test artifact. Section 6.10 accepts it deliberately, acceptance
   check 17 proves the documented end state, and keeping
   `AGGREGATE_BUFFERED_LOW` reachable plus control outside the aggregate are the
   mitigations.
9. **Package version ordering.** Bumping `hub-test-support` before
   `ticket_1788280618_295967` publishes 0.1.42 would strand that coordinate and
   keep the Web consumer blocked. Mitigation: the registered blocking dependency
   and the section 8.1 step order.

## 12. Runtime-teardown lens answers

The class applies. This ticket changes WebRTC peer lifecycle, per-subscription
ownership, admission of a new late-message surface, and channel close paths.

### 12.1 `teardown_class_applies`

Yes. The ticket creates two new peer-created durable ownership surfaces (entity and
package-event reserved routes), a new late `DataChannel` open admission point for
each, and new close and retirement paths that release budget charges.

### 12.2 `teardown_isolation`

- One failed entity or package-event subscription kills exactly one route: its
  reservation, its bounded queue or mailbox, its channel, and its budget charges.
- No sibling subscription is closed, gapped, reordered, or evicted. This is a hard
  requirement from human decision 5.3 and is asserted in acceptance check 9.
- One failed peer kills every route owned by that `peer_generation`, through the
  existing `PeerClosed` sweep extended to entity and package-event reservations.
- Sibling peers keep working on successful close, per
  `[[PeerClosed attach occupancy must use the live attach route set]]`.

### 12.3 `teardown_bounds`

- Every channel close uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND` (3 s production, 200 ms
  test), per `[[WebRTC DataChannel local close uses the peer close bound before cleanup]]`.
- A channel close that exceeds the bound must not block peer cleanup. Hub marks
  the channel closed, retires the route, releases the budget charges, and
  continues.
- The hard stop that ends every channel driver loop is the existing
  `peer_terminal_rx` watch. Each entity and package-event channel loop selects on
  it, so a peer-terminal cause ends the loop without waiting on channel I/O.
- No unbounded `block_on(close)` appears on the Hub control plane.
- `[[a ready WebRTC send must win over a queued DataChannel close]]` is preserved:
  an accepted in-flight send on an entity or event channel resolves before a
  queued close on the same channel.
- Budget release is committed after the fallible close work, so a hung close
  cannot leave a charged slot.

### 12.4 `late_message_matrix`

| Message | Owner tag | Reject after terminal failure | Sweep on race with `PeerClosed` |
|---------|-----------|-------------------------------|----------------------------------|
| `Hello` on the control channel | `grant_id` | `cleanup_sent` gates `RegisterWebrtcAdmission` | Admission never registered |
| `Attach` | `(grant_id, session_id, subscription_id, generation)` | Typed operator error when the peer is dying | Existing detach plus `mux.suppress_generation` |
| `Detach` | same triple | Idempotent no-op | none needed |
| `SubscribeEntities` | `(peer_generation, subscription_id, generation)` | Typed operator error after terminal cause; reserve nothing; charge nothing | `forget_peer` retires the reservation and releases both charges |
| `UnsubscribeEntities` | same | Idempotent; retires the reservation if present | none needed |
| `SubscribePackageEvents` | `(peer_generation, subscription_id, generation)`, with `(owner, name)` as the admitted identity | Typed operator error after terminal cause; reserve nothing; charge nothing | Connection-scoped holder release per `[[Client event holders are connection-scoped]]`, with admitted-job survival per `[[admitted event holders survive producer unload until Core completion]]` |
| `UnsubscribePackageEvents` | same | Idempotent | none needed |
| **`DataChannel` `open` for a reserved entity route (new)** | the reservation label at `peer_generation` | Inspect returns `Unknown`, `Expired`, or `Bound`; Hub closes under the bound and binds no receiver | The reservation registry is the sweep. A late open finds `Unknown` after `forget_peer` and self-closes, releasing the slot |
| **`DataChannel` `open` for a reserved package-event route (new)** | same | same | same |
| **A reserved entity or package-event route that never opens (new)** | same | Reservation expiry after `TERMINAL_RESERVATION_EXPIRES_IN_SECONDS` | `retire_expired` marks it `Expired`, Hub emits the typed close event, and the budget slot is released. Without this the slot leaks against `MAX_SUBSCRIPTION_CHANNELS` |
| Any peer-originated `DaemonRequest` on the control channel | `grant_id` | `local_webrtc_peer_gone_request_error` | none needed |

Both race orders are covered. Open-before-retire binds and then tears down
normally. Retire-before-open finds the reservation `Unknown` or `Expired` at open,
closes the channel, binds nothing, and creates no route.

### 12.5 `production_path_proof`

- Reservation path: browser `SubscribeEntities` or `SubscribeEvents` on the
  control channel, into `daemon/control/entities.rs` or `daemon/control/events.rs`,
  into `ConnectionBudget::charge`, into `SubscriptionReservationRegistry::reserve`,
  out through the control response carrying label and generation.
- Bind path: browser-created channel `open`, into
  `admit_reserved_subscription_channel`, into
  `ControlMessage::InspectReservation`, then the encrypted `Hello` admission, then
  `ControlMessage::BindReservedSubscription`, then the per-class channel host.
- Teardown path: `PeerClosed` or `UnsubscribeEntities` or an entity overflow
  refusal, into the production handler, into route retirement, into budget
  release, into channel close under the bound.
- Oracles are live, not terminal-file only. Each proof drives the production
  handler and then reads a Hub-visible ownership oracle: the reservation state,
  the budget count, and `aggregate_buffered()`. Terminal failure records alone are
  not accepted, per
  `[[terminal webrtc failure records do not prove peer runtime teardown]]`.
- Red-on-revert controls are named in acceptance checks 5, 6, 8, and 9.

### 12.6 `ownership_identity`

- Every peer-created durable row carries `peer_generation`, minted per admitted
  WebRTC grant and monotonic within a Hub process.
- A reused `subscription_id` under a new `peer_generation` cannot bind an older
  reservation. The registry already rejects a `peer_generation` mismatch in
  `lookup_label`, `reservation_for_label`, `expire_label`, and `mark_bound`; the
  class-aware registry keeps that rule.
- A delayed `PeerClosed` snapshot must not delete a row now owned by a live peer.
  `forget_peer` filters on `peer_generation`, so it can only retire its own rows.
- Owner sweeps cover both queue orders: closed-first and message-first.

### 12.7 `sibling_fail_closed_policy`

- On successful close, sibling subscriptions and sibling peers keep working. A
  saturated subscription cannot change a sibling's order, gap state, queue, or
  close state.
- On ultimate local WebRTC close failure the shipped policy stands: every peer on
  the dedicated runtime is sacrificed, per
  `[[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]`.
  This ticket does not change that blast radius and does not silently widen it.
- Entity overflow closes only the overflowing subscription channel. Acceptance
  check 5 asserts that none of the other 30 channels closes.

## 13. Acceptance checks and tests

Every check is a deterministic Rust gate. Wall-clock values are observations only.

1. **Reservation returned.** `SubscribeEntities` and `SubscribeEvents` each return
   a `subscription_reservation` with a non-empty label and the exact minted
   generation, on the control channel, without waiting for channel `open`.
2. **Bind admits only a matching reserved route.** Opening a channel whose label
   matches a `Live` reservation at the peer generation binds. Assert the route
   moves to `Bound`.
3. **Rejection matrix.** Six separate arms, each with its own typed reason and each
   proving the channel closed and nothing bound: late (opened after expiry),
   stale (wrong `peer_generation`), mismatched (label of another class), duplicate
   (second open of a `Bound` label), unreserved (never-reserved label), and
   over-limit. Each arm must fail on its own guard, so no arm may be satisfiable by an
   earlier guard. A row that an earlier guard already rejects proves nothing, and
   each typed reason must be distinct.
4. **Open timeout retires and releases.** Withhold `open` past the reservation
   expiry. Assert the typed close event, the reservation reads `Expired`, and the
   budget count returns to its pre-request value.
5. **Entity overflow, artifact rows E1 through E4 with assertions 5 and 5b.**
   Follow artifact section 14.3 exactly and strictly serially: admit 31 entity
   channels while `aggregate_buffered()` is 0; fill 29 at 65,536 B and 2 at
   98,304 B including `C_cross`, reaching exactly 2,097,152 B; attempt the 32nd
   subscription **first**; then attempt the 65,536 B send on `C_cross`. Assert:
   (1) the 32nd subscription receives a typed aggregate admission rejection while
   a free channel slot exists; (2) `aggregate_buffered()` is still exactly
   2,097,152 B; (3) the send is refused before the write (E1); (4)
   `aggregate_buffered()` is still exactly 2,097,152 B at refusal; (5) the
   handler-boundary trace records E2 before E3 and E4, with `aggregate_buffered()`
   read at E2 still exactly 2,097,152 B; (5b) the client receives the typed
   `entity_subscription_overflow` reason for `C_cross` on the control channel,
   with no ordering requirement relative to E3 or E4; (6) none of the other 30
   channels closes; (7) after E4, `aggregate_buffered()` is 1,998,848 B and a
   fresh subscribe is admitted.
   **Red-on-revert control:** return the control channel to the aggregate budget
   and assert assertion 5 fails first, because a saturated budget refuses the very
   response that reports the saturation.
6. **No budget drift on any failure path.** For each of reservation, bind
   rejection, open timeout, replacement, close, peer loss, and retirement, assert
   the channel count and the derived aggregate return to their pre-transition
   values. Red-on-revert: remove one release site and assert this check fails
   first.
7. **One table, one path.** A source guard asserts the section 9 constants are
   defined only in `src/admission/connection_budget.rs`, with a required-symbol
   anchor per `[[region bounded source guards need a required symbol anchor]]`, and
   one ablation per added file per
   `[[fixed source guard lists need one ablation per added file]]`.
8. **Isolation, with a surviving-channel positive control.** Saturate one entity
   channel, then assert terminal frames still leave in order on a sibling terminal
   channel, and that a package-event flood delays neither terminal nor control.
   Per `[[rejected channel isolation needs a surviving channel positive control]]`,
   first wait for a known payload marker on the surviving channel in the same
   traffic window, and prove every isolation channel reached `Open`, before any
   zero-frame assertion on the rejected channel.
9. **Package-event isolation, reserve, and fair admission.** Five named arms from
   human decision `question_1788281660_218527`, plus the ceiling arm:
   a. One subscription alone uses the full 128-event and 2 MiB depth.
   b. A later `SubscribeEvents` is rejected with one typed capacity reason while
      its 4-event or 65,536-byte reserve is unavailable. Assert no existing
      subscription was evicted, gapped, closed, or shrunk, and that no waiting
      admission queue exists.
   c. After capacity drains, that later subscription is admitted and keeps its
      reserve.
   d. A noisy admitted subscription cannot consume an admitted sibling's reserve:
      the sibling still accepts its full 4 events and 65,536 bytes while the noisy
      one sits at its own limit.
   e. Count and byte accounting do not drift across every teardown path:
      rejection rollback, timeout, close, replacement, and retirement, each
      released exactly once.
   f. With 32 admitted subscriptions, total residency never exceeds 128 events and
      2 MiB.
   Red-on-revert: restore the earlier `floor(cap / live_N)` push-time share and
   assert arm (d) fails first, because a subscription admitted alone then occupies
   a later sibling's guarantee.
10. **Control writer does not starve lifecycle events.** Under a continuous
    control request stream, assert a pending lifecycle close event still reaches
    the client, and that the writer emits a bounded number of frames per turn.
11. **Move proof.** Paired absence and presence source guards per
    `[[code moves need paired absence and presence source guards]]`: assert
    `src/host_control_fair_write.rs` is absent and returns zero grep matches, that
    the scheduler symbols are present in `src/transport/unix/host_write_order.rs`,
    that `HostControlClass::Entity` and `entity_ready` are absent repository-wide,
    and that the entity and event framing symbols left
    `src/transport/webrtc/control_channel.rs` and entered
    `src/transport/webrtc/subscription_channel.rs`. No forwarding wrapper remains.
12. **DTO gates, wire proof and source proof kept separate.**
    a. *Serde wire proof:* `subscription_reservation` round-trips, is omitted when
       `None`, and an older client that does not know the field still decodes the
       response.
    b. *Downstream source proof:* build representative consumer worktrees through
       a local Cargo `[patch]` redirect, not the workspace path. The consumer must
       be TUI-shaped per
       `[[tui shaped Hub consumer proofs must include hub test support]]`: it
       declares `botster-hub-client`, `botster-ui-contract`, and
       `botster-hub-test-support` as a dev-dependency, and it is built in a mode
       that actually compiles the dev-dependency. A client-only consumer cannot
       observe a `botster-hub-test-support` build failure.
    c. *Client cost measurement:* record both the TUI all-target cost and the Web
       exact-artifact cost per
       `[[a ui contract import line change costs one test line in each generic client]]`.
    d. *Package gates, only after the section 8.1 dependency publishes 0.1.42:*
       regenerate and assert the TypeScript, set `CONFORMANCE_FIXTURE_REVISION`
       48, take `@trybotster/hub-test-support` 0.1.43 as a new unpublished
       version, update every version, protocol, and revision literal in
       `packages/hub-test-support/test.mjs`, then run `npm install --no-save` and
       `npm test` in `packages/hub-test-support`.
    e. *Clean-consumer smoke:* resolve the installed scoped package through its
       exported root entrypoint, not `package.json`, per
       `[[clean consumer smokes resolve exported root entrypoints not package json]]`.
13. **Live Hub proof.** Prove the contract through the exact Hub binary, recording
    the Hub SHA and its lockfile-pinned worker Core SHA separately, per
    `[[live hub proof records distinct hub and locked core binary provenance]]`.
14. **Official gates**, in this order, from a colon-free worktree with
    `CARGO_TARGET_DIR` unset, per `[[Hub official gates must not set CARGO TARGET DIR]]`:
    - `RUSTUP_TOOLCHAIN=1.97.0 rustc --version`, recorded
    - `RUSTUP_TOOLCHAIN=1.97.0 cargo fmt --all -- --check`
    - `RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings`,
      rerun in full after each repair per
      `[[strict clippy can hide later crate diagnostics behind the first compile failure]]`
    - `RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked -p botster-core-daemon --bin botster-session-worker`
    - `RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --bin botster-hub`
    - `RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked`

15. **A26, the aggregate does not drift.** Reach the exact ceiling with the
    section 14.3 setup, drain every channel fully, and assert
    `aggregate_buffered()` returns to 0 and held classes resume. Red-on-revert:
    replace the derived sum with a counter incremented on send and decremented on
    `bufferedAmountLow`, and assert this check fails first, because the low-water
    event carries no byte delta and transport drains are never subtracted.
16. **A27, a refused terminal send is backpressure, not loss.** Substitute one
    terminal channel for one of the 29 entity channels in the section 14.3 setup,
    keeping 31 channels and exactly 2,097,152 B, so the free-slot and ceiling
    properties of check 5 are preserved. Then offer one 65,536 B terminal frame
    and assert, in order: the aggregate predicate refuses it; `try_write` returns
    `WouldBlock` and not `Full`; `pressure()` is `WouldBlock`; Core retains the
    frame while the Hub adapter does not; `aggregate_buffered()` is still exactly
    2,097,152 B; and after draining below 1,048,576 B the same frame is delivered
    byte-exact, in order, with no duplicate from the refused attempt. Drive at
    most 8 attempts before the drain, two orders of magnitude below Core's 512, so
    a slow drain on a loaded runner cannot silently convert this into a teardown
    test. Also assert Hub drops, reorders, and retries no terminal frame at any
    point in the sequence.
17. **A27b, sustained pressure reaches Core's documented hard stop.** Hold
    aggregate pressure through 512 consecutive unsuccessful attempts without
    draining, and assert Core calls `hard_stop`, emits `ClientWorkerTeardown`, and
    Hub retires the corresponding route. This proves the real end state instead of
    asserting retention that the Core contract does not provide.

Every exact `cargo test` filter uses the full module path and shows a one-test
baseline before an ablation loop, per
`[[exact Rust test ablations require a one test baseline]]`. A bare leaf name
filters out every test and still reports `ok`, which turns an ablation arm falsely
green. Every source mutation is completed and restored before the official locked
gate starts, per `[[source guard ablations must not overlap a running full suite]]`.

**Artifact rows carried by this ticket:** A5, A6, A7, A8b, A9 (partially, see
6.8), A10, A25, A26, A27, and A27b. Every row that depends on the section 9 budget
is now owned here, because this ticket builds that budget. Checks 3, 4, and 6
cover A7 and A8b; check 8 covers A5, A6, and A10; check 5 covers A25; checks 15,
16, and 17 cover A26, A27, and A27b. Rows A4, A8, and A8c already landed through
the merged cold cut `ticket_1787894427_525056` for the terminal class, and checks
2 and 3 extend the same guards to the entity and package-event classes. No row
that depends on this ticket's budget is left unowned.

## 14. Vault gaps worth capturing

1. **A superseded ticket can strand a frozen architecture obligation.** Section 9
   of `artifact_1787606577_290231` was assigned to a ticket that closed as
   superseded, and the successor cold cut declared it non-scope, so the obligation
   fell into a gap that only a plan-time grep found. Candidate note: a frozen plan
   section whose owning ticket is superseded needs explicit reassignment before the
   successor closes.
2. **A frozen artifact can be superseded in part by a later vault decision.**
   Artifact sections 8.2 and 8.3 are superseded by
   `[[the browser creates each subscription DataChannel after Hub reserves its label]]`
   and by shipped opaque labels, while sections 9, 11, and 14.3 stay binding.
   Candidate note: how to record partial supersession of a frozen contract so a
   later planner does not re-adopt a corrected section.
3. **A whole-file deletion instruction needs a repository-wide condition check.**
   Artifact section 12.3 ordered deletion of `src/host_control_fair_write.rs` "once
   only the Control class remains" and forbade an escape, but the condition was
   false because Unix still uses two classes. Candidate note: file-deletion
   acceptance rows must state the scope in which their condition is evaluated.
4. **Per-connection bounds do not survive per-subscription isolation for free.**
   Splitting one shared mailbox into 32 isolated mailboxes multiplies worst-case
   residency unless a connection ceiling with reserved shares is added. Candidate
   note: isolation splits must restate the aggregate bound and the fair-share rule
   together.
