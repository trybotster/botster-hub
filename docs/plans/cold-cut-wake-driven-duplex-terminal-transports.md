# Hub: cold-cut wake-driven duplex terminal transports

Ticket: `ticket_1787894427_525056`
Run: `run_1788046974_604085`
Plan visit: 4 (Plan Review returned the run at `review_1788059091_852326`, changes required)

This plan replaces `docs/plans/drive-terminal-progress-from-core-and-adapter-wakes.md`,
which the human decision superseded. That file stays as the historical record.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Repository path of record: the routed `botster-hub` run worktree.
- Base ref: `main`. Verification base commit: `1664312` on branch
  `project-pipelines/ticket_1787894427_525056`, clean tracked worktree.
- The target repository comes from the ticket `target_id` through `list_spawn_targets`,
  not from the process working directory.

## Repository playbook loaded

- [[botster-hub-playbook]] — repository ownership charter for `botster-hub`.
- [[botster-hub-client-playbook]] — loaded because this cold cut deletes public
  `DaemonRequest` terminal variants and changes the `Attach` response contract.

## Other role and surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Required architecture and surface context (added after finding `finding_1788048365_989149`):

- [[botster-architecture]]
- [[cli-patterns]]

Class overlay (runtime-teardown class applies):

- [[botster runtime teardown lenses]]

Atomic notes:

- [[core terminal progress is wake driven and targeted]]
- [[core waking terminal adapters shipped at revision ec589ee]]
- [[terminal adapters emit coalesced writable and closed wakes]]
- [[core ingress wake sources are transport neutral]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[core default requirement includes duplex binary again]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[the browser creates each subscription DataChannel after Hub reserves its label]]
- [[webrtc 0 21 restores post handshake DataChannel creation in Hub]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[rejected channel isolation needs a surviving channel positive control]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[an overflow reconcile walk must reuse the readiness filter it backstops]]
- [[session wake coalescing belongs in a lifecycle registry not each handle]]
- [[session ingress wakes retire on observed exit not shutdown acceptance]]
- [[count before publish or a concurrent counter cannot be exact]]
- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[code moves need paired absence and presence source guards]]
- [[exact Rust test ablations require a one test baseline]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Unix mux host frames flush before new terminal slots]]
- [[Unix mux host events are unsolicited control frames]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[generated typescript dtos must encode serde field optionality]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[WebRTC host events use unsolicited daemon-event delivery]]

No other repository charter is loaded. No Project Pipelines overlay is required.

## Context loaded

- Vault capture: `ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md`.
- Human decision `question_1788048108_946530`, revised answer: no backwards compatibility,
  no compatibility window, no feature filter, no mixed pin, no fallback, no second active
  terminal route. This Hub ticket is the single Hub-side cold cut.
- Plan Review `review_1788059091_852326` (changes required, this visit),
  `review_1788058432_516678`, `review_1788054175_439108`, and `review_1788048365_936269`.
  Every open finding is addressed below.
- Hub source: `src/daemon/owner_loop.rs`, `src/daemon_maintenance.rs`,
  `src/daemon/control/sessions.rs`, `src/subscription/attach_routes.rs`,
  `src/subscription/closed_events.rs`, `src/transport/shared/*`, `src/transport/unix/*`,
  `src/transport/webrtc/*`, `src/runtime.rs`, `src/main.rs`, `src/lib.rs` source guards,
  `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Hub gates: `test.sh`, `docs/hub-resource-proof.md`, `docs/lifecycle-suite-harness.md`,
  `tests/hub_daemon_lifecycle/*`.
- Dependency repository source at `botster-core` `origin/main` =
  `786f61c5aeec42b416826af6ca0b4be9f3cc3c0f`, including
  `docs/architecture/core-daemon.md`, `docs/architecture/terminal-adapter.md`,
  `crates/botster-core/src/contract/terminal_adapter.rs`,
  `crates/botster-core/src/contract/terminal_wake.rs`,
  `crates/botster-terminal-protocol/src/input_frame.rs`.

### Load-bearing facts established from the code

1. `src/daemon_transport.rs` is gone. Decomposition steps 1 through 6 are complete.
2. The only production Hub path that pumps a bound adapter today is
   `run_pump_observe_phase` -> `HubRuntime::observe_lifecycle_slice` ->
   `CoreDaemon::observe_lifecycle_slice` -> `observe_session` -> `drain_runtime_once`.
   `HubRuntime::drain_runtime_once` and `drain_subscription` are already `#[cfg(test)]`
   and forbidden in production sources by the `src/lib.rs` scanner.
3. At the new pin, `observe_lifecycle_slice` **retains** incidental terminal egress and
   **does not pump bound adapters**. `read_screen`, `read_mode_flags`, `capture_snapshot`,
   and `capture_color_and_snapshot` also do not pump bound adapters. Therefore the pin roll
   alone would stop all Hub terminal output: the pin roll and the driver are one cold cut.
4. `TerminalAdapter` at the new pin is duplex. It adds `try_read(&mut self) -> TerminalIngress`
   with `Empty`, `Frame(Vec<u8>)`, `Lost`, `Closed`. `Lost` is fail-closed: Core hard-stops
   that owner. A conforming adapter buffers at least
   `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` (64) complete frames. The current Hub adapters
   implement only `try_write`, `close`, and `pressure`, so both must gain a real ingress
   buffer. `CoreDaemon::pump_woken` intakes named routes and does not `try_read` an unnamed
   adapter.
5. Terminal input on the wire is `botster_terminal_protocol::TerminalInputFrame`: a
   four-byte header (scheme version `1`, kind `1` input / `2` mode-gated input / `3` resize,
   `u16` body length) plus an opaque body. Hub validates the header only and must not decode
   the body. Core owns semantic decode.
6. `TerminalCompatibility::current()` at `786f61c` requires
   `transport=duplex_binary` in the default required-feature list. There is no legacy
   requirement path to select.
7. WebRTC terminal output today is encrypted and sent on the **control** DataChannel by
   `flush_webrtc_adapter_frames` -> `send_response_frames`. `subscription_channel.rs`
   currently only **rejects** extra DataChannels through `reject_extra_data_channel`.
8. `DaemonRequest::Attach` binds the adapter immediately inside
   `src/daemon/control/sessions.rs`. A browser-created subscription channel requires a
   two-phase Attach: reserve the label, return it, bind when the labeled channel opens and
   passes admission.
9. Hub pins `webrtc = "0.21.0-beta.2"`, so post-handshake channels open. The product
   contract still puts channel creation in the browser.
10. Hub close-event delivery already runs off the owner thread. Only classification and
    queueing run on the owner loop today, as a full walk over every admission and route in
    `run_close_events_phase`.
11. The JSON terminal routes have live Hub-side callers: `src/main.rs` `sessions send-input`
    and `sessions resize`, and `DaemonRequest::{SendInput, ModeGatedInput, Resize}` plus
    `DaemonResponseKind` variants in `crates/botster-hub-client`, mirrored into
    `crates/botster-hub-client/generated/daemon-protocol.ts`,
    `packages/hub-test-support/daemon-protocol.ts`, and
    `packages/hub-test-support/first-party-client-support-matrix.json`.

## Scope

### 1. Core pin roll (one family, one revision)

Pin every `botster-core` dependency to
`786f61c5aeec42b416826af6ca0b4be9f3cc3c0f`. No mixed revisions. This revision
adds the supported single-owner wake-pump seam from Core dependency
`ticket_1788220245_689733`.

Surfaces: `Cargo.toml` (5 rev literals), `crates/botster-hub-test-support/Cargo.toml` (3),
`crates/botster-hub-client/Cargo.toml` (1),
`crates/botster-hub-test-support/build.rs` `PROTOCOL_REV`,
`crates/botster-hub-test-support/src/conformance_data.rs` `LATE_ATTACH_GHOSTSNP_CORE_PIN`,
`crates/botster-hub-test-support/src/lib.rs`, `tests/session_projection_owner_loop.rs`
`REQUIRED_CORE_REV`, `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`
`LOCKED_CORE_REV`, the four provenance literals in
`tests/hub_daemon_lifecycle/{unix_terminal_adapter,webrtc_terminal_adapter,event_plane_saturation,package_event_plane}.rs`,
and the six `Cargo.lock` sources. Require zero `7eafa47` matches outside `docs/plans`
and `docs/reports`.

### 2. Duplex waking adapters (shared mechanics)

In `src/transport/shared/adapter_slot.rs` and a new
`src/transport/shared/ingress.rs`:

- Store the Core `TerminalWakeSink` installed through `WakingTerminalAdapter::set_wake_sink`.
- Emit one coalesced `Writable` wake when the write slot drains (`complete_active`) **and**
  when a new complete ingress frame is buffered, because `TerminalWakeKind::Writable` means
  "the adapter has work Core should pump".
- Emit one `Closed` wake on close. Close stays idempotent and never emits a later `Writable`.
- Add a bounded ingress buffer holding at least `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` (64)
  complete frames. On overflow the adapter latches `Lost` and reports `TerminalIngress::Lost`
  once; after close, `try_read` is permanently `Closed` and buffered ingress is dropped.
- Validate only the `TerminalInputFrame` header before buffering. Reject a malformed header
  as a transport protocol error and close that route. Never decode the body.

`UnixTerminalAdapter` and `WebRtcTerminalAdapter` then implement `TerminalAdapter::try_read`
and `WakingTerminalAdapter` through this shared inner. Hub stays content blind.

### 3. Wake-driven data-plane driver

New `src/data_plane.rs` and `src/data_plane/driver.rs`. One thread named
`botster-hub-data-plane` constructs, owns, mutates, shuts down, and drops `CoreDaemon`.

- The thread calls `wake_pump_control()` once and returns only its thread-safe control handle.
- The thread blocks in `wait_pump(WATCHDOG)` and calls `pump_woken` for real wake batches.
- `HubRuntime` sends bounded host operations through a 64-slot request channel.
- A host request marks pending work and interrupts only a blocked wait. It never gains direct
  daemon access.
- One turn executes at most 64 host requests and eight route-close keys.
- `request_stop()` interrupts the wait. The owner pumps the one permitted collision batch,
  observes `WakePumpWait::Stopped`, finishes accepted requests, and then performs Core shutdown.

No `CoreDaemon`, `Rc`, or `RefCell` value crosses a thread boundary. Hub adds no unsafe
`Send` or `Sync` implementation and no shared mutable daemon wrapper.

### 4. Route-specific close progress (repairs `finding_1788048365_134036`)

The previous bounded-queue-plus-flag design could drop an exact route key. Replace it with a
**close-work registry that mirrors the Core wake source**, in
`src/data_plane/close_work.rs`:

- `CloseWorkSource` owns a registry of `Arc<RouteCloseState { queued, retired, key }>`, one
  per bound route, created at `mux.register` and retired at route teardown.
- The adapter close hook holds a `Weak<RouteCloseState>` plus a bounded `SyncSender`.
  Close does one CAS on `queued`, then a non-blocking send. If the channel is full, it sets
  an overflow flag and **leaves `queued` set**.
- `CloseWorkSource::take_batch` drains the channel, then, only when the overflow flag was
  set, walks the registry and emits exactly the route states whose `queued` is true and
  whose `retired` is false. This reuses the readiness filter it backstops, per
  [[an overflow reconcile walk must reuse the readiness filter it backstops]].
- Recovery therefore never touches the admission maps and cannot fabricate close work for an
  unrelated route. Every exact `(session_id, subscription_id, generation)` key survives.
- The driver classifies each key with `CoreDaemon::session_registry_state` through the
  existing `session_close_event_decision` mapping and queues the closed event on that route
  only. `ClosedEventRoute.reported` keeps idempotency.

`run_close_events_phase` and `PumpPhase::CloseEvents` leave the owner loop.

### 5. WebRTC terminal cold cut

- Keep one `RTCPeerConnection` and one reliable ordered control DataChannel per browser peer.
- `DaemonRequest::Attach` on a WebRTC peer becomes two phase. Hub admits the subscription,
  reserves an exact label bound to `(session_id, subscription_id, generation, peer_generation)`,
  and returns that reservation on the control channel. Hub does not bind yet.

#### 5a. Exact Attach reservation contract

`DaemonResponseKind` gains one variant, `TerminalReservation`. `DaemonResponse` gains one
optional field in its existing flat-struct style:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub terminal_reservation: Option<DaemonTerminalReservation>,
```

```rust
/// Hub-reserved subscription DataChannel label for one admitted terminal route.
///
/// Non-exhaustive so later additive fields stay source-compatible for external
/// Rust consumers, per [[public dto field additions are source breaking without non exhaustive]].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DaemonTerminalReservation {
    pub session_id: String,
    pub subscription_id: String,
    /// Core-minted terminal subscription generation for this route.
    pub generation: u64,
    /// Hub peer generation that owns the reservation.
    pub peer_generation: u64,
    /// Exact DataChannel label the peer must create. Opaque to the peer.
    pub label: String,
    /// Whole seconds the peer has to open the labeled channel.
    pub expires_in_seconds: u32,
}
```

Transport behavior:

- **WebRTC.** A successful `Attach` returns `kind = TerminalReservation` with
  `terminal_reservation = Some(..)` and binds nothing. The peer then creates the labeled
  channel. Terminal frames start only after Hub admits and binds that channel.
- **Unix.** A successful `Attach` keeps its current response kind and returns
  `terminal_reservation = None`. Unix binds inline on the existing connection, exactly as
  today. The field is absent on the wire because of `skip_serializing_if`.
- **Expiry retirement.** No new timer thread. A reservation whose `expires_in_seconds` has
  passed retires lazily, at whichever comes first: the next event that touches its route, or
  the existing bounded owner-loop inventory reconcile slice. Retirement is what bounds the
  resource; the deadline alone is not a correctness mechanism.

##### Two error codes with two different delivery paths

The two codes are **not** symmetric, and only one of them can be an `Attach` response.

- **`reservation_label_conflict` — synchronous, on the `Attach` response.** Allocation can
  fail when a live, non-retired reservation already exists for the exact
  `(session_id, subscription_id, generation, peer_generation)`, which a repeated `Attach`
  can produce. Hub reserves nothing and returns the existing
  `DaemonResponseKind::OperatorError` shape from `attach_bind_operator_error` with
  `operation = "attach"` and this code. No new envelope is introduced.
- **`reservation_expired` — asynchronous, on the late-channel admission path.** Expiry
  happens *after* a successful `Attach` response, so it can never be an `Attach` response.
  A late channel open is not a control request. Its delivery path is:
  1. Hub retires the reservation atomically, so a concurrent open cannot bind.
  2. Hub emits one unsolicited `TerminalSubscriptionClosed` daemon event on the **peer's
     control DataChannel**, for the exact `(session_id, subscription_id, generation)`, with a
     new reason constant `TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED =
     "reservation_expired"` alongside the existing
     `TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER` and
     `TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER`. This follows
     [[WebRTC host events use unsolicited daemon-event delivery]] and
     [[Unix mux host events are unsolicited control frames]].
  3. Hub then bounded-closes the late channel through the existing
     `reject_extra_data_channel` path.
  That order tells the peer why before its channel dies, and it cannot bind in the gap. If
  the control channel is already gone, the event is dropped and only the bounded close runs.
  A late channel whose label was never reserved keeps the plain unknown-label rejection with
  no event, so **stale and unknown stay distinguishable**.
- **Serde.** `label` is an opaque string. Hub never derives peer-visible meaning from its
  contents, and no client parses it.
- The browser creates one reliable ordered DataChannel with the reserved label. Hub accepts a
  remote-initiated channel **only** when its label matches a live `Reserved` route, applies
  the existing encrypted DataChannel Hello admission, then binds the duplex Core adapter.
- The subscription channel carries opaque terminal output and opaque
  `TerminalInputFrame` input for that one subscription. Encryption, chunking, bounds,
  late-open fencing, stale-generation closure, and pressure isolation stay per channel.
- Delete `flush_webrtc_adapter_frames`, the control-channel terminal path, and the WebRTC
  terminal mux. Terminal frames and terminal input never cross the control channel.
- An unreserved or stale-label channel keeps the existing `reject_extra_data_channel`
  bounded-close path.

### 6. Unix terminal cold cut

- Bind each admitted Unix terminal subscription to the same duplex Core adapter.
- The Unix terminal route carries Core binary input, mode-gated input, resize, output,
  attach frames, pressure, and close. Preserve existing Unix framing mechanics, the
  host-frame-before-new-terminal-slot rule, and bounded transport shutdown.
- Terminal input arrives on the same connection as an opaque `TerminalInputFrame` addressed
  to a bound route, and reaches Core only through adapter `try_read`.

### 7. Deletion (no fallback, no second route)

- Delete `DaemonRequest::{SendInput, ModeGatedInput, Resize}`, their
  `DaemonResponseKind` variants, their handlers in `src/daemon/control/sessions.rs`, and
  their classification arms in `src/daemon/control.rs`,
  `src/daemon/control/request.rs`, `src/transport/webrtc/control_channel.rs`, and
  `src/local_webrtc_smoke.rs`.
- Delete the polling terminal progress path: the terminal side of
  `run_pump_observe_phase`, and `HubRuntime::bind_terminal_adapter` usage.
- Delete the shared control-channel terminal path inside Hub.
- **Bump `PROTOCOL_VERSION` from 7 to 8.** Deleting request variants and changing the
  `Attach` response are breaking request and response changes.
  [[daemon event shape changes bump conformance fixture revision not protocol version]]
  limits conformance-only bumps to compatible shape changes, so a revision bump alone would
  let a protocol-7 client pass admission and then call an absent route or misparse the
  `Attach` response. Update `crates/botster-hub-client/src/lib.rs` `PROTOCOL_VERSION`,
  `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` handling, every compatibility fixture and
  descriptor, `crates/botster-hub-test-support/src/lib.rs`, the generated TypeScript,
  `docs/client-protocol.md`, and the protocol assertions in
  `tests/hub_daemon_lifecycle/{shutdown,sessions,unix_terminal_adapter,webrtc_terminal_adapter}.rs`
  (`unix_terminal_adapter.rs` and `webrtc_terminal_adapter.rs` each assert the literal `7`).
  Three unrelated constants share the substring `PROTOCOL_VERSION` and must **not** change:
  `src/mcp.rs` `MCP_PROTOCOL_VERSION` (`"2025-06-18"`) is the Model Context Protocol version
  and carries no daemon literal, `botster_terminal_protocol::PROTOCOL_VERSION` is the
  terminal protocol plane, and `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` is the
  conformance floor. Touch only `botster_hub_client::PROTOCOL_VERSION`.
  Also bump `CONFORMANCE_FIXTURE_REVISION` from 46 for the fixture changes: the two signals
  are separate and both apply.
- Regenerate `crates/botster-hub-client/generated/daemon-protocol.ts`,
  `packages/hub-test-support/daemon-protocol.ts`, and
  `packages/hub-test-support/first-party-client-support-matrix.json`, and bump the
  conformance fixture revision, per
  [[daemon event shape changes bump conformance fixture revision not protocol version]].
- CLI and client-helper disposition, decided by human answer `question_1788057545_301065`
  (option A, atomic inside this repository):
  - Delete the `botster-hub sessions send-input` and `botster-hub sessions resize`
    subcommands, their `SessionAction` variants, their argument parsing, and their CLI help.
  - Delete the `botster-hub-client` one-shot helpers for those requests, the
    `DaemonRequest::{SendInput, ModeGatedInput, Resize}` variants, and every
    `DaemonResponseKind` variant that exists only for them.
  - Terminal input and resize then exist **only** through a bound duplex Unix or WebRTC
    terminal subscription.
  - Add no attach-send-detach compatibility path, deprecated wrapper, hidden alias,
    fallback, or tombstone variant.
  - Update the authoritative Rust DTO inventory, generated TypeScript, fixtures, CLI help,
    examples, documentation, and source guards in the same merge.
  - Remove tests that prove only a deleted one-shot route. Preserve or add tests that prove
    input, mode-gated input, and resize through a bound duplex route.
  - Review must verify that no production caller and no public re-export of the deleted
    variants remains.

### 8. Owner-loop reduction

`run_pump_observe_phase` keeps bounded lifecycle observation and journal-wake handling only,
per [[Hub owner loop calls bounded Core lifecycle page APIs]]. `PumpPhase` reduces to
`InventoryReconcile` plus `Observe`. Entity reconciliation, inventory reconciliation,
package-event delivery, maintenance slices, and read-only control work stay on the owner
loop unchanged.

### 9. Observability (repairs `finding_1788048365_651815`)

Data-plane progress counters are an **internal, `BOTSTER_ENV=test` gated diagnostic seam**,
written to a caller-named file in the style of the existing
`BOTSTER_HUB_TEST_EXTRA_CHANNEL_OBSERVATION` seam. They are not new public
`DaemonLifecycleCounters` fields. The only public DTO change in this ticket is the terminal
route deletion and the Attach reservation, both of which the cold cut requires.

### 10. Test seams and source guards

- `BOTSTER_ENV=test` gated Hub child settings: pause the driver, force adapter `WouldBlock`,
  set a long watchdog interval, and force close-work channel overflow.
- Add `src/data_plane.rs`, `src/data_plane/driver.rs`, `src/data_plane/close_work.rs`, and
  `src/transport/shared/ingress.rs` to the fixed `src/lib.rs` production-scan file list,
  with one ablation per added file.
- Paired absence and presence guards for each moved responsibility.

### Non-scope

- `botster-web` and `botster-tui` client implementation. Those remain their own tickets and
  are registered as consumers of this merge.
- Entity and package-event dedicated DataChannels.
- Replay buffers or terminal sequence replay.
- Transport crate extraction. Concrete transports stay Hub modules.
- Removing the Core polling bind from the Core repository.

## Repository ownership boundaries and cross-repository dependencies

- `botster-core` owns terminal subscription identity, generations, attach phases, ordering,
  bounded queues, pressure, fencing, teardown, the duplex adapter contract, input semantics,
  the wake contract, and the targeted pump. Hub adds no terminal semantics and decodes no
  input body.
- `botster-hub` owns admission, grants, labels, reservations, peer generations, budgets,
  route state, the concrete Unix and WebRTC transports, the hosting process, and the host
  wait loop.
- `botster-hub-client` owns the external DTO boundary. This ticket changes it, so
  [[botster-hub-client-playbook]] gates apply: generated TypeScript, support matrix,
  fixtures, and downstream consumer proof.
- Dependencies, all closed:
  - `ticket_1787894424_927579` (botster-core waking adapters).
  - `ticket_1787894965_150479` (Hub decomposition 4b).
  - `ticket_1788054075_697438` (Core: restore the direct duplex requirement) — output is
    `botster-core` `b292c38`.
  - `ticket_1788220245_689733` (Core: expose a supported thread-safe wake-pump host seam) —
    output is `botster-core` `786f61c` and supersedes the earlier Hub mutex design.
- Downstream consumers `botster-web` and `botster-tui` must be registered as dependent
  tickets against their own repository targets, because this merge removes the JSON terminal
  routes they use today.

## Runtime-teardown class

`teardown_class_applies`: **yes**. The ticket changes WebRTC peer and channel lifecycle,
adapter close paths, route teardown ownership, multi-subscription ownership, and adds a
long-lived host thread to the terminal byte path.

`teardown_isolation`
- One route's ownership set: the Core route wake state, the Core subscription, the Hub
  `RouteCloseState`, the Hub route record, the adapter slot with its ingress buffer, and —
  for WebRTC — that subscription's own DataChannel.
- One route close must not touch a sibling route, a sibling subscription channel, a sibling
  session, or another peer. Core hard-stops only the blocked route after its bounded
  rejected-pump budget and preserves siblings.
- A failed **connection or peer** still kills every route it owns, unchanged and intentional,
  because those routes share the socket or the peer.

`teardown_bounds`
- The driver never blocks on transport I/O. `wait_pump` uses a watchdog timeout that is a
  hang bound, not a progress mechanism.
- One data-plane thread owns `CoreDaemon`. A bounded request bridge serializes Hub host work
  on that same thread. No mutex shares the daemon across threads.
- Per-turn work is bounded by explicit constants: pumped routes, close keys, and ingress
  frames drained.
- WebRTC channel and peer close keep the existing `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and always
  reach cleanup, per [[WebRTC DataChannel local close uses the peer close bound before cleanup]].
- Hard stop, stated exactly, because `JoinHandle::join` has no timeout:

  1. **Stop signal.** `HubRuntime::release_for_restart` stops request admission, selects the
     restart-release action, and calls `WakePumpControl::request_stop()`. This call interrupts
     an in-flight `wait_pump(WATCHDOG)`.
  2. **Completion signal.** The Core owner pumps the one permitted stop-collision batch,
     observes `WakePumpWait::Stopped`, finishes accepted requests, performs the selected Core
     shutdown or restart release, and sends one completion signal. Hub waits with
     `rx.recv_timeout(DATA_PLANE_STOP_BOUND)`, then joins the completed owner thread.
  3. **Bound derivation.** `DATA_PLANE_STOP_BOUND = 2 * DATA_PLANE_WATCHDOG + STOP_SLACK`.
     The loop body is non-blocking except for `wait_pump`, and every per-turn budget is a
     fixed constant, so the worst-case exit is one in-flight `wait_pump` plus one bounded
     turn. A timeout therefore signals a defect, not ordinary load.
  4. **Terminal action on timeout.** Hub does not call `join()` and does not continue the
     shutdown sequence, because the Core owner thread can still be live. Hub records the
     typed `data_plane_driver_stop_timeout` diagnostic, then
     calls `std::process::abort()`.

  **What abort does and does not do.** Abort guarantees exactly one thing: every Hub thread,
  including a driver that will not stop, ends with the process. It runs no Rust destructor
  and no Hub cleanup, so it explicitly does **not** remove the Unix socket file and does
  **not** stop or reap session-worker processes. Claiming otherwise would make this proof
  internally inconsistent, so the plan states the residue and its owner instead:

  | Residue after abort | Intentional? | Who recovers it |
  |---|---|---|
  | `botster-hub-data-plane` and every other Hub thread | ended by abort | nothing to recover |
  | Session-worker processes | **yes, by design** | the next Hub start, through the existing `adoption_scan` and `adopt_session` path. Workers already outlive a Hub crash; restart adoption is the established recovery, not a leak. |
  | Stale Unix socket file | yes | the next Hub start, through the existing `prepare_socket_path`, which probes the path and removes a socket with no live daemon behind it |
  | Core durable state | yes | Core owns its own recovery on restart |

  **Timeout sibling policy.** The driver stop timeout is the one deliberate whole-process
  fail-closed action in this plan. It sacrifices every peer and every route on this Hub,
  because a Core owner thread that did not complete makes any narrower teardown unsound.
  That is a wider blast radius than a route close or a peer close.

  Acceptance check 16 covers the normal path. Check 16a asserts only what abort actually
  guarantees, in process, and check 16b proves the recovery half separately on the next Hub
  start.

`late_message_matrix`

| Message or event | Owner tag | Rejection after terminal failure | Residual sweep |
|---|---|---|---|
| `Attach` (reserves a route) | `client_id, session_id, subscription_id, generation`, plus `peer_generation` for WebRTC | `live_generation_for_route` returns `None`, then `fail_closed_pre_bind_attach` | existing pre-bind fail-closed path |
| Reserved-label channel open | reserved label bound to the exact route and peer generation | label unknown, stale, already bound, or Hello admission fails | `reject_extra_data_channel` bounded close; the reservation expires with its route |
| Waking duplex bind | same key plus the Core rejection ladder before wake-state allocation | Core returns `BindTerminalAdapterError`; Hub closes the handle and the channel | wake state allocated only after every check passes |
| Terminal input frame | the bound route that owns the channel or Unix slot | malformed header, unknown route, stale generation, or closed adapter rejects and closes that route | ingress buffer dropped on close |
| Ingress overflow | route ingress buffer | adapter latches `Lost`; Core hard-stops that owner | route retired, sibling routes untouched |
| `Writable` wake after close | route wake state, retired on hard stop | closed slot must not emit `Writable` | driver pump is a no-op for a route Core no longer holds |
| Close-work key | `session_id, subscription_id, generation` | `reported` plus the suppression set reject a second event | registry overflow walk emits only queued, non-retired states |
| `Detach`, `ShutdownSession`, `RemoveSession` | exact `(session_id, subscription_id, generation)` | existing exact-key suppression before Core shutdown | unchanged, never session-wide |
| `SubscribeEntities` / package-event subscribe | connection-scoped, owner loop | unchanged | unchanged; never enters the driver |
| Ingress wake for a stopping session | `SessionId` in the Core live-session registry | wake stays live through `Stopping`; `ProcessExited` or runtime removal retires it | Core `forget_session` after teardown commits |

`production_path_proof`
- Output: PTY or worker output -> Core ingress wake -> `CoreDaemon::wait_pump` ->
  `CoreDaemon::pump_woken` -> `try_write` on the exact bound
  adapter -> `AdapterSlot` -> Unix mux writer or that subscription's DataChannel -> client.
- Input: client `TerminalInputFrame` -> Unix terminal route or that subscription's
  DataChannel -> adapter ingress buffer -> adapter `Writable` wake -> driver ->
  `CoreDaemon::pump_woken` -> Core `try_read` and semantic decode -> PTY.
- Close: adapter close -> `Closed` wake to Core and one CAS-guarded key into
  `CloseWorkSource` -> driver -> route-specific closed-event queue -> transport writer ->
  `TerminalSubscriptionClosed`.
- Oracles are the live `tests/hub_daemon_lifecycle` suite against the real `botster-hub`
  binary and the real session worker. Each proof names a red-on-revert control.

`ownership_identity`
- Every durable route row keeps `client_id, session_id, subscription_id, generation`, and
  WebRTC rows also keep `peer_generation`.
- The reserved label and the close-work key both carry the generation, so a delayed channel
  open or a delayed close key cannot bind to, report, or delete a row now owned by a
  replacement subscription that reuses the `subscription_id`.
- Both queue orders are covered: closed first then replacement binds, and replacement binds
  first then the stale key or stale channel arrives.

`sibling_fail_closed_policy`
- On successful route close, sibling subscription channels, sibling routes on the same
  connection, and other peers keep working. Required test.
- On ultimate close failure of one adapter, Core hard-stops that route after its bounded
  rejected-pump budget and preserves siblings; Hub emits the core-adapter closed event for
  that route only.
- The existing local-WebRTC ultimate close failure sibling-sacrifice policy on the dedicated
  runtime is unchanged and is not weakened.

## Assumptions and unknowns

Assumptions:

1. The pin is `botster-core` `786f61c`, the output of dependency
   `ticket_1788220245_689733`. `TerminalCompatibility::current()` there requires
   `transport=duplex_binary` by default, so Hub advertises duplex with no feature filter.
2. Hub proves the WebRTC subscription-channel contract with its own test peer in
   `tests/hub_daemon_lifecycle`, because browser implementation is non-scope. The test peer
   creates the reserved-label channel exactly as the browser will.
3. `pump_woken` performs adapter intake for named routes, so input needs no separate Hub
   pump call.
4. The driver starts inside `HubRuntime` construction so the daemon and the in-process
   lifecycle harness take the same production path. Implement may move it into
   `serve_daemon` only with evidence that no in-process production path needs it.
5. `MAX_OWNER_TURN_MS` stays 25 and the 64-thread Hub bound stays unchanged.

Unknowns:

- **U1 — resolved.** `question_1788057545_301065` answered: option A. Delete the two CLI
  subcommands, the one-shot client helpers, and the three `DaemonRequest` variants with
  their request-only response kinds. No compatibility path, wrapper, alias, fallback, or
  tombstone. The `botster-hub-client` DTO deletion and the regenerated
  `daemon-protocol.ts` are in scope for this same Hub merge; the cold cut is atomic inside
  this repository.
- **U2.** Whether `node packages/hub-test-support/scripts/sync-assets.mjs --check` needs a
  regenerated asset set after the pin roll and the DTO deletion.
- **U3.** Which existing in-crate tests bind an adapter and rely on
  `observe_lifecycle_slice` for byte progress. Each conversion must keep its original
  assertion and subject.
- **U4.** Mutex contention shape between the driver and the owner loop under the event-plane
  saturation campaign. Report a regression rather than raising `MAX_OWNER_TURN_MS`.

## Affected surfaces and files

New:

- `src/data_plane.rs`, `src/data_plane/driver.rs`, `src/data_plane/close_work.rs`
- `src/transport/shared/ingress.rs`

Changed:

- `Cargo.toml`, `Cargo.lock`
- `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/generated/daemon-protocol.ts`
- `crates/botster-hub-test-support/{Cargo.toml,build.rs,src/lib.rs,src/conformance_data.rs}`
- `packages/hub-test-support/daemon-protocol.ts`,
  `packages/hub-test-support/first-party-client-support-matrix.json`
- `src/lib.rs`, `src/runtime.rs`, `src/main.rs`, `src/local_webrtc_smoke.rs`
- `src/daemon/owner_loop.rs`, `src/daemon_maintenance.rs`, `src/daemon/control.rs`,
  `src/daemon/control/request.rs`, `src/daemon/control/sessions.rs`
- `src/subscription/attach_routes.rs`, `src/subscription/closed_events.rs`
- `src/transport/shared/{adapter_slot.rs,wake.rs}`
- `src/transport/unix/{adapter.rs,connection.rs,mux_write.rs}`
- `src/transport/webrtc/{peer.rs,control_channel.rs,subscription_channel.rs,delivery.rs,adapter.rs,test_support.rs}`
- `docs/hub-resource-proof.md`, `docs/client-protocol.md`, `README.md`
- `tests/hub_daemon_lifecycle/*`, `tests/session_projection_owner_loop.rs`

## Risks

1. **Cold cut size.** Pin roll, duplex adapters, driver, WebRTC channel topology, Unix input,
   and route deletion land in one merge. A partial landing attaches but delivers nothing.
   Mitigation: the acceptance list gates the merge; no fallback route exists to mask a gap.
2. **Two-phase Attach regression.** Reserving then binding widens the window in which a route
   exists without an adapter. Mitigation: the reservation carries the generation and expires
   with its route; proofs 8 and 12 cover stale and late opens.
3. **Ingress buffer as a hidden policy queue.** The 64-frame buffer must stay a transport
   buffer, not a retry or reorder queue. Mitigation: `Lost` is latched and fail-closed;
   guard tests assert no retry and no reorder.
4. **Host-request pressure.** Owner-loop Core operations cross the bounded data-plane request
   bridge. The wake-pump interrupt prevents watchdog-scale request latency.
5. **Downstream breakage.** `botster-web` and `botster-tui` stop working against this Hub
   until their own tickets land. Mitigation: register both as dependent tickets and state the
   break explicitly; the human decision accepts it.
6. **WebRTC channel leak.** A reserved label whose channel never opens, or a channel whose
   route retires, must not pin a peer or a `RouteCloseState`. Mitigation: `Weak`
   back-references, reservation expiry, and the resource-bound proof.
7. **Close-work registry growth.** A route state that never retires leaks. Mitigation:
   retirement at route teardown plus the resource-bound counters returning to zero.
8. **Shutdown race.** The Core owner must observe `WakePumpWait::Stopped` before it calls
   `CoreDaemon::shutdown`. Hub joins only after that owner reports completion.
9. **Conformance revision churn.** Deleting DTO variants changes the fixture revision and
   every generated consumer artifact.

## Acceptance checks and tests

Every live proof runs against the real `botster-hub` binary and the real session worker in
`tests/hub_daemon_lifecycle`. Exact Cargo filters use the full module path and show a
one-test baseline before each ablation.

1. **Terminal bytes progress while the owner loop is idle.** Bytes arrive while
   `reconciliation_wakes` and `lifecycle_session_drains` do not advance and the test-gated
   data-plane observation records the pump. Red-on-revert: remove the driver start.
2. **Generic control and readback cannot drive terminal progress.** With the driver paused,
   `Status`, `ListSessions`, `ReadScreen`, `ReadModeFlags`, `CaptureSnapshot`, and shutdown
   classification produce zero terminal frames; resuming delivers the retained frames in
   order. Red-on-revert: restore an owner-loop terminal pump.
3. **Writable, ingress, and closed wakes target only affected subscriptions.** With two live
   subscriptions, a wake on one must pump exactly one route. Red-on-revert: pump all routes.
4. **A full adapter resumes from its writable wake.** Force `WouldBlock`, clear pressure,
   assert one coalesced `Writable` wake resumes delivery with the watchdog set long enough to
   exclude a timer. Red-on-revert: drop the `Writable` emission.
5. **Ingress progresses without a correctness timer.** Watchdog far above the test deadline;
   worker and PTY output still arrive.
6. **WebRTC and Unix terminal input use Core binary frames.** On each transport, send
   `input`, `mode-gated input`, and `resize` as `TerminalInputFrame` bytes on the terminal
   route and assert the PTY observes them. Assert a malformed header closes only that route.
   Assert Hub never decodes a body: a source guard forbids semantic input decode in Hub
   production sources.
7. **Terminal bytes never cross the WebRTC control DataChannel.** Observe expected terminal
   payload on the subscription channel in the same window, then assert zero terminal frames
   on the control channel, per
   [[rejected channel isolation needs a surviving channel positive control]].
   Red-on-revert: restore `flush_webrtc_adapter_frames`.
8. **Reserved-label admission.** A channel with an unreserved label, a stale label, a stale
   peer generation, or a failed encrypted Hello is rejected and bounded-closed, and never
   binds. A correctly reserved label binds and delivers.
9. **A slow subscription cannot block a sibling.** Jam one adapter `Full` while a sibling on
   the same peer and the same session keeps receiving; then exhaust the Core rejected-pump
   budget and assert the blocked route hard-stops while the sibling survives.
10. **Close is route-specific and idempotent, including under overflow.** Exactly one
    `TerminalSubscriptionClosed` for the exact `(session_id, subscription_id, generation)`,
    no sibling event, no second event on repeated close. Force close-work channel overflow
    and assert every queued route still reports exactly once and that no unrelated route
    receives close work. Red-on-revert: restore the admission-wide close scan.
11. **Attach-frame retention through the waking duplex bind.** Declare an attach before bind,
    then bind; assert `Attaching` -> `Snapshot` (when non-empty) -> `Attached` ->
    `TerminalOutput` with no dropped or duplicated frame.
12. **Stale generations cannot bind, wake, close, or replace a live route**, covering a stale
    reserved-label open and a stale close key in both queue orders.
13. **Control precedence is a decision-level oracle** (repairs `finding_1788048365_841983`).
    Assert `classify_owner_poll` returns `ServeControl` ahead of a due background slice, that
    no data-plane work runs on the owner thread, and that per-turn pumped routes, close keys,
    and drained ingress frames stay within their explicit constants. Live control latency and
    `max_owner_turn_us` are recorded as observations under a stated load band, not as the
    pass/fail oracle.
14. **Reconnect and late open.** A peer reconnects, re-reserves, and re-opens its subscription
    channels; the previous generation cannot deliver or close the new route.
15. **Resource bounds.** `focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload`
    plus an updated `docs/hub-resource-proof.md`: exactly one added `botster-hub-data-plane`
    thread, at most 64 Hub OS threads, unchanged queue capacities, close-work and route
    counters returning to zero, and an unchanged idle CPU bound. The driver must block, not
    spin.
16. **Shutdown ordering, normal path.** Assert the owner observes
    `WakePumpWait::Stopped`, completes the selected Core shutdown action, and sends the
    completion signal within `DATA_PLANE_STOP_BOUND`. Assert that `join()` then returns. A
    thread census shows no `botster-hub-data-plane` thread, and an
    orderly down leaves no Hub thread, session worker, zombie, or socket. Exact-key
    suppression still holds for admitted Unix and WebRTC routes before Core shutdown.
    Red-on-revert: call `CoreDaemon::shutdown` before the stopped state; Core must return
    `CoreDaemonError::WakePump`.
16a. **Shutdown timeout path, deterministic.** A `BOTSTER_ENV=test` seam parks the driver
    inside its turn so it cannot reach the completion send. On the isolated Hub child, assert
    that the wait ends at `DATA_PLANE_STOP_BOUND`, that `CoreDaemon::shutdown` is never
    called (proved by the absence of its observation marker while the driver-timeout marker
    is present), that the typed `data_plane_driver_stop_timeout` diagnostic is recorded, and
    that the process terminates by abort. The in-process assertion is limited to what abort
    actually guarantees: the process is gone and **no Hub thread survives**. It does not
    assert socket removal or worker reaping, because abort runs no destructors and cannot
    perform them. Red-on-revert: replace the abort with a plain `join()`; the test must hang
    or observe a surviving thread.
16b. **Abort residue recovery, on the next start.** After the aborted child, assert the
    documented residue and its recovery rather than pretending abort cleaned up:
    - the session workers survive, which is the intended restart-adoption behavior;
    - the stale socket file is still present;
    - a fresh Hub start then removes that stale socket through `prepare_socket_path` and
      adopts the surviving workers through `adoption_scan` and `adopt_session`;
    - after that start, the worker set is adopted, not duplicated, and no zombie remains.
    Red-on-revert: make the timeout path attempt `CoreDaemon::shutdown` first; the
    never-called assertion in 16a must fail.
17. **Deletion is complete.** Source guards prove `SendInput`, `ModeGatedInput`, and `Resize`
    JSON terminal routes are absent from Hub production sources, that no second active
    terminal route exists, and that `pump_woken(` appears in exactly one production file,
    `src/data_plane/driver.rs`. Paired presence guards prove each moved symbol reached its new
    owner.
18. **Source guard inventory.** The four new files are in the fixed production-scan list, with
    one ablation per added file.
19. **Client contract proof.** Regenerated `daemon-protocol.ts`, the hub-test-support package
    copy, and the support matrix match the Rust DTOs; the conformance fixture revision is
    bumped; the hub-test-support Node test passes.
19b. **Protocol 8 admission.** Assert exact protocol equality separately from the conformance
    revision floor: a protocol-7 client is rejected at admission and never reaches an absent
    route or an unparsed `Attach` response, while a protocol-8 client at the same conformance
    revision is admitted. Red-on-revert: leave `PROTOCOL_VERSION` at 7; the protocol-7
    rejection assertion must fail.
19c. **Attach reservation contract.** Prove the exact serde shape on both transports: a
    WebRTC `Attach` returns `kind = TerminalReservation` with every
    `DaemonTerminalReservation` field present and binds nothing until the labeled channel
    opens; a Unix `Attach` omits `terminal_reservation` from the wire entirely and binds
    inline. The two error codes are proved on their two different paths:
    - `reservation_label_conflict` through the real `Attach` handler: a repeated `Attach`
      for a route that already holds a live reservation returns `OperatorError` with
      `operation = "attach"` and this code, and reserves nothing.
    - `reservation_expired` on the late-channel admission path, not through `Attach`: after
      the deadline passes and the reservation retires, opening the labeled channel emits one
      unsolicited `TerminalSubscriptionClosed` on the peer's control DataChannel with reason
      `reservation_expired` for the exact `(session_id, subscription_id, generation)`, then
      bounded-closes the channel, and the channel never binds. A channel whose label was
      never reserved is rejected with no event, which is what keeps stale distinguishable
      from unknown.
    Red-on-revert: emit `reservation_expired` as an `Attach` response; the asynchronous
    delivery assertion must fail.
19d. **Pack and consume the real downstream artifacts.** Name and exercise the exact paths
    Web and TUI consume, rather than only the in-repo generated copies:
    - Node: `npm pack` `packages/hub-test-support` and install that tarball into a scratch
      consumer that imports `@trybotster/hub-test-support/daemon-protocol` and
      `/first-party-client-support-matrix`. Assert the deleted request identifiers are absent
      and the reservation type is present.
    - Rust: use a scratch Cargo patch redirect against the real `botster-web` and
      `botster-tui` worktrees, per
      [[scratch cargo patch redirects measure downstream dto breakage]], without
      contaminating their primary checkouts.
    Both consumers are expected to **fail** on this cut. Record each expected failure exactly:
    the consumer, the command, and the compile or admission error it produces. That recorded
    failure is the evidence that the break is understood and scoped, and it is the acceptance
    input for the dependent `botster-web` and `botster-tui` tickets. An unexpected passing
    consumer is a finding, because it would mean a deleted route survived.
19a. **No survivor of the deleted routes.** A guard proves that `SendInput`,
    `ModeGatedInput`, and `Resize` request identifiers appear in no Hub production source, no
    `botster-hub-client` public re-export, no generated TypeScript, no fixture, no CLI help
    text, no example, and no documentation page. Tests that proved only a deleted one-shot
    route are removed, and bound-route tests for input, mode-gated input, and resize replace
    them on both transports.
20. **Pin-roll completeness.** Zero prior active Core SHA matches outside `docs/plans` and `docs/reports`;
    all six `Cargo.lock` sources rolled; every active revision literal rolled; one Core family
    revision everywhere.

Gate commands, recording `rustc --version` from the same shell:

```sh
RUSTUP_TOOLCHAIN=1.97.0 cargo fmt --all -- --check
RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked -p botster-core-daemon --bin botster-session-worker
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --bin botster-hub
RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked
(cd packages/hub-test-support && npm install --no-save && npm test)
```

Gate hygiene: the official `./test.sh --locked` gate runs in a colon-free worktree with
`CARGO_TARGET_DIR` unset, and prebuilds the Hub and worker into the default worktree
`target/`. The lifecycle suite needs a quiet host.

## Vault gaps worth capturing

1. **Hub drives the Core data plane from one owned driver thread** — the thread constructs
   and exclusively owns `CoreDaemon`; other Hub threads use bounded requests and
   `WakePumpControl`; the owner observes stopped before Core shutdown and thread completion.
2. **`observe_lifecycle_slice` retains terminal egress and does not pump bound adapters at the
   published revision** — the fact that makes the pin roll a cold cut.
3. **Hub close work uses a registry with a queued and retired filter, not an admission scan** —
   the overflow walk must reuse the readiness filter it backstops.
4. **Hub adapters buffer opaque input frames and validate only the header** — Hub stays
   content blind on the input direction as well as the output direction.
5. **Two-phase WebRTC Attach: Hub reserves the label, the peer opens it, Hub binds** — the
   first shipped Hub implementation of the frozen topology.
6. **Pin the supported wake-pump revision `786f61c`** — it preserves the direct duplex
   requirement and removes the need for any Hub unsafe thread override.

Capture is deferred to Verify so no unproven design enters the vault.
