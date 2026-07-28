# Eliminate unbounded polling connection and entity-subscription lifecycles

## Target and context

- Target repository: `botster-hub`.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline run: `run_1785199801_176415`, Plan step
  `botster_stack_plan`.
- Repository charter: [[botster-hub-playbook]].
- Role and surface playbooks loaded: [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]],
  [[botster-hub-client-playbook]], and [[project-pipelines-playbook]].
- Architecture maps and required planner context loaded:
  [[botster-architecture]], [[cli-patterns]], [[spa-patterns]],
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[plan agents must author vault context as wikilinks not home paths]], and
  [[vault example paths are not repository placement conventions]].
- Hub ownership notes loaded: [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[webrtc bootstrap origin must be requested after the package server binds]],
  [[daemon socket attach must detach subscriptions on disconnect and exit]],
  [[accepted unixstreams from nonblocking listeners must restore blocking mode for line readers]],
  [[hub daemon runtime stays on one owner thread while socket handlers submit requests]],
  [[daemon request errors should return operator frames without dropping transport]],
  [[daemon session cleanup should report typed cleanup frames for shutdown races]],
  and [[daemon shutdown disconnects count as success only after clean owned process exit]].
- Client/reconnect notes loaded: [[botster entity snapshots are authoritative reconnect baselines]],
  [[botster client subscriptions should not hydrate global state]],
  [[subscription ID namespacing separates TUI and browser clients]],
  [[attach reconnect must drop stale outbound requests before resubscribe]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[adding a hub client feature constant is a three site change]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  and [[generated typescript dtos must encode serde field optionality]].
- The two package-supervision notes were loaded because the Hub charter requires
  them, but they do not constrain this ticket because no manifest,
  `may_supervise`, entrypoint admission, or launch policy changes. The WebRTC
  origin note does constrain `src/local_webrtc.rs`: cleanup counter wiring must
  preserve post-bind bootstrap issuance, exact-origin validation, and the
  existing `LocalWebrtcPeerState::cleanup_once` behavior.
- Repository evidence inspected: `README.md`, `Cargo.toml`, `Cargo.lock`,
  `test.sh`, `.github/workflows/loaded-daemon-lifecycle.yml`,
  `src/daemon_transport.rs`, `src/local_webrtc.rs`, `src/daemon.rs`,
  `src/runtime.rs`, `src/client_api.rs`,
  `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`,
  `crates/botster-hub-test-support/src/lib.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`, and the
  current CoreDaemon lifecycle API pinned by `Cargo.lock`.
- The active repository convention remains `docs/plans/`; this plan follows
  the current tree and mainline prior art.

## Current production path and failure

The user path is:

`botster-hub` daemon binary -> `serve_daemon` -> Unix socket connection
adapter -> `ControlMessage` -> `HubClientApi` -> `HubRuntime` ->
`CoreDaemon` -> `botster-session-worker`.

The browser path enters the same control owner through the local WebRTC adapter.
Terminal bytes remain on the SessionIo/ClientWorker data plane.

The current Unix adapter creates one detached OS thread for every accepted
connection. A held-open entity stream wakes in `recv_timeout` every 20 ms and
then calls zero-timeout `poll(2)`, so every idle client adds 50 wakeups per
second. The daemon owner also runs a 20 ms accept/control/lifecycle loop. When
even one entity subscription exists, every owner tick calls
`session_lifecycle_baseline()`. At the pinned Core revision that takes the
CoreDaemon mutex and calls `registry.load_all()`: `fs::read_dir`, then
`fs::read` and JSON deserialization for every persisted session record. The
same tick then calls `drain_runtime_once` for each non-terminal session. The
steady-state cost is therefore 50 full registry scans and per-live-session
drains per second. It scales with session count even when subscriber count is
fixed and is a likely dominant source of the reported idle CPU.

Cleanup is spread across return branches: EOF and some write failures detach
or unsubscribe, while malformed frames, handshake failures, control-channel
loss, early entity-subscribe write failures, shutdown, and unwind can bypass
one or both cleanups. There is no hard admission bound, joined connection
owner, or typed live/high-water/cleanup evidence.

CoreDaemon exposes a bounded lifecycle baseline/change journal but no blocking
lifecycle notification receiver at the pinned revision. Hub must seed one
reconciliation cursor/baseline and active-session set, consume
`session_lifecycle_changes(&cursor)` during steady state, and call the
filesystem-backed `session_lifecycle_baseline()` only when
`resync_required` demands it. A single bounded backstop still must drive
`drain_runtime_once` for the remembered live sessions to discover natural
exits, but it must not rescan the registry on every wake, become one timer per
subscriber, or recreate lifecycle truth.

## Binding product decision

Question `question_1785200067_696253` established:

- Healthy idle entity and attach streams remain open indefinitely.
- Do not add an absolute lease or deliberately churn correct clients.
- Bound admitted/live connections and subscriptions explicitly.
- Apply deadlines/cancellation to handshake, incomplete frames, stalled
  writes/slow consumers, shutdown, and demonstrably dead transports.
- Use a standard failure-triggered liveness mechanism where the transport can
  detect a dead peer.
- Reconnect after actual transport loss creates a fresh subscription and
  authoritative entity snapshot; this ticket does not add periodic reconnect
  protocol behavior.

## Scope

1. Replace detached per-accept threads with one daemon-owned, joined async Unix
   transport reactor using the repository's existing Tokio dependency. Keep
   non-`Send` `HubRuntime`/CoreDaemon state on its single owner task; connection
   tasks submit discrete bounded requests and never borrow runtime state.
2. Add an explicit fixed Hub admission bound for live Unix connection tasks and
   bounded request/egress queues. Reject excess connections promptly with
   typed, payload-free diagnostics rather than spawning work or waiting
   indefinitely.
3. Convert held-open entity delivery to an async select over inbound socket
   frames, bounded entity frames, cancellation/shutdown, and write deadlines.
   No per-client interval, `recv_timeout`, zero-timeout readiness probe, or
   thread is allowed.
4. Replace branch-by-branch cleanup with one connection ownership record/guard.
   It owns every terminal attach and entity subscription registered by that
   transport and emits exactly one high-priority cleanup command on EOF,
   malformed/incomplete frame, read/write failure, cancellation, admission
   teardown, normal close, daemon shutdown, and panic/unwind. The daemon owner
   performs detach/unsubscribe and records whether cleanup succeeded, was
   already complete, or failed.
5. Keep one shared, bounded lifecycle reconciliation pump for CoreDaemon
   natural-exit discovery. Seed its cursor and in-memory live-session set from
   one baseline, use `session_lifecycle_changes(&cursor)` on every steady-state
   pass, update the set from ordered upsert/remove changes, and call
   `session_lifecycle_baseline()` only after an explicit `resync_required`.
   Drive immediately after relevant control requests; when only the natural
   exit backstop remains, back off to a low fixed idle cadence independent of
   subscriber count and reset on activity. The backstop may drain remembered
   live sessions, but it must not perform a full registry scan per wake. Count
   change reads, baseline/resync reads, per-session drains, and wakes separately
   so tests can prove steady-state baseline reads stay at zero and idle cost
   stays flat across both subscriber and session-count axes.
6. Apply bounded handshake and incomplete-frame read deadlines and the existing
   bounded write policy. A healthy frame-complete idle held-open stream has no
   expiry. Unix EOF/HUP/reset and WebRTC close/error remain failure-triggered
   liveness signals.
7. Add sanitized Hub runtime counters for cumulative accepted/rejected
   connections, current/high-water live connections, current/high-water entity
   and attach subscriptions, actual reconnect registrations, cleanup outcomes
   by terminal reason/disposition, reconciliation wakes, entity delivery
   attempts/successes/overflow/failures, and stalled writes. Define reconnect
   classification explicitly from observable subscription generations; do not
   infer user identity or expose subscription/session ids.
8. Publish the counters through the existing daemon status/debug surface using
   typed `botster-hub-client` DTOs. Preserve old-daemon deserialization with
   serde defaults and generated TypeScript optionality. A DTO/fixture-shape
   change bumps the conformance fixture revision, not the framing protocol
   version; do not add a required feature constant unless behavior is genuinely
   unavailable without it.
9. Bring local WebRTC peer cleanup and Unix cleanup into the same counter and
   once-only outcome vocabulary while preserving the existing
   `LocalWebrtcPeerState::cleanup_once` behavior, post-bind origin-bound
   bootstrap issuance, exact-origin validation, and browser data-channel flow.
10. Extend real-daemon test support and the loaded lifecycle campaign with
    transport/counter observations that prove the production binary path, not
    source shape or elapsed sleeps alone.

## Non-scope

- No `botster-core`, session-worker, PTY, terminal renderer, or terminal-history
  ownership changes.
- No legacy monolith changes, compatibility fallback path, second daemon
  transport, or parallel lifecycle registry.
- No botster-web or botster-tui repository edits in this run. Hub tests must
  exercise their real transport shapes; downstream adoption changes become
  separately targeted tickets only if verification finds an actual consumer
  incompatibility.
- No periodic healthy-client disconnect, absolute stream lease, new heartbeat
  dialect, global list-refresh fallback, or hydration outside entity frames.
- No configurable queue/timeout tuning surface unless existing Hub
  configuration already owns the value. Use small, documented Hub policy
  constants and test their behavior.
- No package/plugin, Project Pipelines workflow, UI, or unrelated daemon module
  refactor.

## Ownership boundaries and dependencies

- `botster-hub` owns admission limits, local transport scheduling, deadlines,
  cancellation, cleanup policy, diagnostics, and the single runtime owner.
- `botster-core`/CoreDaemon remains the policy-free lifecycle and terminal
  mechanism. The pinned API has no blocking lifecycle notification seam, so no
  Core prerequisite is currently required. If implementation cannot meet the
  idle-wakeup acceptance without a reusable Core notification primitive, stop
  and register a dependency against target
  `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`; do not add Core code here.
- The in-repository `botster-hub-client` crate owns public status/debug DTOs,
  serde compatibility, helpers, and generated TypeScript. It does not own
  server admission policy.
- `botster-hub-test-support` owns downstream-shaped subprocess proof and
  published conformance assets. Generated Node copies remain derived artifacts.
- SessionIo/ClientWorker own terminal bytes and slow-client data-plane
  isolation. The reactor may bound socket egress but must not buffer an
  unbounded second terminal history.
- The closed UiNode extraction dependency on this ticket is already satisfied
  and is unrelated to this transport change.
- Web and TUI are consumers. Their real reconnect shapes are acceptance inputs,
  not permission to broaden this repository run.

## Assumptions and unknowns

- Assumption: existing Tokio is the smallest framework primitive that can wait
  on socket input, bounded channel delivery, and cancellation without one
  thread or timer per connection. Enable only the required `net`/`io-util`
  features.
- Assumption: the admission cap and queue sizes are Hub policy constants, not
  new user configuration. Choose values from production-shaped Web/TUI
  concurrency evidence and assert them by counter/high-water behavior.
- Assumption: a reconnect counter can be defined without a new stable client
  identity by counting a new held-open subscription generation after a prior
  generation for the same transport-visible subscription class was cleaned up.
  The final name and classification must be documented and covered by Web/TUI
  churn tests; ordinary one-shot control connections must not inflate it.
- Assumption: the single shared natural-exit reconciliation backstop may use an
  adaptive fixed cadence because the current Core API has no event receiver.
  Its steady-state operation uses cursor changes and the remembered
  live-session set, never a full baseline unless resync is required. Its idle
  rate and measured work must stay within the live CPU/wakeup threshold across
  both client and session counts.
- Unknown: exact safe admission, queue, deadline, and idle-backoff constants.
  Measure them with the existing real daemon harness; do not expose optional
  tuning or weaken ticket cases to fit a guessed value.
- Unknown: whether status counters should be one always-present debug struct or
  an optional backwards-compatible struct. The authoritative Rust serde and
  generated TypeScript must agree, old fixture JSON must deserialize, and live
  Hub status must always expose the fields needed by acceptance.
- Unknown: whether a downstream Web/TUI drift check fails solely because the
  generated status DTO changes. If so, create a follow-up against that
  repository target rather than editing it here.
- No engineering convention conflict or waiver is known.

## Affected surfaces and likely files

- `Cargo.toml` and `Cargo.lock` — only required Tokio feature resolution; no new
  transport crate.
- `src/daemon_transport.rs` — async listener/reactor, bounded admission and
  channels, connection owner/guard, event-driven entity streaming, shared
  reconciliation scheduling, counters/status projection, and focused units.
- `src/local_webrtc.rs` — report peer/subscription lifecycle through the shared
  once-only cleanup/counter vocabulary without replacing its async adapter.
- `src/daemon.rs` and `src/lib.rs` — only if lifecycle counters need a
  daemon-owned public snapshot type or re-export.
- `crates/botster-hub-client/src/lib.rs` — typed sanitized debug counter DTO and
  status compatibility defaults.
- `crates/botster-hub-client/src/typescript.rs` and
  `crates/botster-hub-client/generated/daemon-protocol.ts` — authoritative
  generated TypeScript and optionality checks.
- `crates/botster-hub-test-support/src/lib.rs` and generated
  `packages/hub-test-support` assets — only where the public status/conformance
  shape changes or a reusable live resource report is added.
- `tests/hub_daemon_lifecycle_test.rs` — production subprocess/socket/WebRTC
  cases and resource baselines.
- `script/run-loaded-daemon-lifecycle` and
  `.github/workflows/loaded-daemon-lifecycle.yml` — add a focused bounded
  connection/reconnect campaign if the existing lifecycle-suite selector
  cannot preserve the required counter/process evidence.
- `README.md` and `docs/client-protocol.md` — document connection limits,
  cleanup ownership, counter semantics, healthy-idle behavior, and reconnect
  baseline.
- This plan file — update it if Plan Review accepts a different bounded
  implementation detail without changing the ticket contract.

## Implementation sequence

1. Introduce the typed connection/subscription counter snapshot and internal
   once-only cleanup reason/disposition model with deterministic unit tests.
2. Move Unix accept/read/write handling to the daemon-owned Tokio reactor,
   retain a single runtime owner, and enforce bounded admission/request/egress.
3. Route all Unix terminal paths through one guard-owned cleanup command,
   including failures before and after response delivery.
4. Replace the entity stream poll loop with async socket/frame/cancellation
   selection and wire overflow/write failure to the same guard.
5. Make the Core lifecycle reconciliation schedule shared and activity-driven
   with cursor changes as the steady-state path, resync-only filesystem
   baselines, and a low idle backstop; preserve terminal egress returned by
   targeted Core drains.
6. Feed local WebRTC cleanup outcomes into the same counters without changing
   its public transport semantics.
7. Project status DTOs, regenerate/check TypeScript and package artifacts, and
   document exact counter definitions.
8. Add focused deterministic transport tests, then real subprocess resource
   proof and loaded repetition before the full strict gates.

## Risks

- Moving a large synchronous transport module to async can stall all clients
  if `session_lifecycle_baseline()` performs registry filesystem I/O or
  `drain_runtime_once` performs per-session work while holding the CoreDaemon
  mutex on a connection task. All CoreDaemon-mutex work stays on the single
  owner task and is never awaited from a connection task. The steady-state path
  uses the in-memory lifecycle journal; the filesystem baseline is resync-only.
  If any Core/filesystem call must move off the owner, it uses
  `spawn_blocking` and returns a discrete result without moving the non-`Send`
  runtime. A deterministic large/slow-registry test must prove an unrelated
  control client remains bounded.
- A bounded cleanup channel can deadlock exactly when cleanup is required. Use
  a dedicated high-priority once-per-admitted-connection path whose maximum
  pending messages is proven by the admission bound; cleanup must not wait on
  normal request or egress capacity.
- Guard state applied before a failed attach response can detach a subscription
  that never registered; applied after a successful response write can leak on
  write failure. Registration ownership must follow the owner response, not
  client delivery success, and cleanup must be idempotent.
- Duplicate entity/attach ids can cause one connection to clean another's
  resource. Registration and cleanup must bind ids to connection generation,
  reject duplicates without replacing the prior owner, and test the original
  stream remains live.
- Slow consumers can fill bounded egress. Control frames must fail and clean up
  predictably; entity overflow must retain the established authoritative
  resnapshot-or-close contract; terminal history must not move into Hub.
- A shared reconciliation timer can still burn idle CPU or delay natural-exit
  frames. Counter deltas and process CPU/wakeup evidence must set a measurable
  non-scaling threshold while preserving bounded lifecycle latency.
- Shutdown can race response delivery, cancellation, and guard drop. Preserve
  the existing independently awaited clean daemon-exit rule and join/drain all
  admitted tasks before removing the socket.
- Public counter fields can drift from generated TypeScript or expose ids,
  paths, commands, or payloads. Publish counts and bounded reason labels only,
  with serde/generator and PII scans.
- Timing-only tests can pass without exercising failure paths. Use socket pairs,
  explicit buffer saturation/shutdown, counter transitions, process/thread
  observations, and independently awaited child status.

## Acceptance checks and downstream proof

### Deterministic transport and ownership tests

- A raw Unix client that disconnects abruptly after attach returns the current
  attach count to baseline, increments exactly one EOF cleanup outcome, and
  cannot receive later terminal output.
- A client that half-writes a JSON frame hits the incomplete-frame deadline;
  a malformed complete frame hits protocol cleanup; both release all
  connection-owned subscriptions and leave a later status request healthy.
- Force response and entity-frame write failure with a socket pair or blocked
  reader, without sleep-only inference. Assert the stalled-write deadline,
  once-only cleanup, counter outcome, and unrelated client responsiveness.
- Reuse duplicate attach and entity subscription ids from another connection.
  The duplicate is rejected, the original owner remains live, and closing the
  rejected connection cannot clean the original resource.
- Saturate the admitted connection bound with live/half-open clients. Assert
  OS thread count remains within a fixed baseline delta, live/high-water never
  exceed the cap, excess connections are rejected, and an existing admitted
  client remains usable.
- Cancel/unwind a handler at each registration stage and assert exactly one
  cleanup command and no negative/current-counter drift.
- Stop the daemon with ordinary, attach, entity, malformed, and slow clients
  present. Assert all tasks join, current connection/subscription counters
  reach zero, the owned child exits cleanly, and the socket is removed.

### Real Hub/Core/session-worker path

- Extend `tests/hub_daemon_lifecycle_test.rs` or the reusable isolated harness
  with rapid TUI-shaped Unix reconnect and Web-shaped local WebRTC reconnect
  churn. Each actual loss must clean the old generation, the new entity
  subscription must begin with an authoritative snapshot, and entity state
  must remain frame-only.
- Run the existing slow terminal consumer path together with entity
  subscriptions and ordinary control requests. The slow client must time out
  or resync/close by policy while unrelated clients keep bounded response
  latency.
- Run a two-axis idle matrix after counters settle:
  subscribers `1..N` at a fixed session count, then sessions `1..M` at a fixed
  small subscriber count using the four-package/many-session shape from sibling
  ticket `ticket_1785199716_875648`. Delivery and per-connection wake counters
  must remain unchanged without events; steady-state baseline/resync reads must
  remain zero; shared reconciliation wakes, per-session drains, process CPU,
  and the wake/context-switch proxy must stay flat within an explicit measured
  tolerance across both axes. If the low-frequency natural-exit backstop's
  per-session drain count grows with `M`, that growth must not translate into
  material steady-state CPU or wake growth and must not regain 50 Hz ×
  subscribers or 50 Hz × sessions scaling.
- Build a large persisted session registry and make its resync baseline
  deliberately slow or observable. While that baseline runs on the owner path,
  prove the reactor remains live and an unrelated admitted control client is
  either served within the documented bound or receives the explicit bounded
  busy outcome; it must not hang behind hidden connection-task mutex work.
- Capture process evidence from the spawned production binary: daemon PID,
  baseline and peak OS thread counts, CPU time or platform-available context
  switches/wake proxy, public counter snapshots before/during/after churn, and
  independently awaited clean shutdown. A regex scan or sleep-only assertion
  is not acceptance.
- Repeat the focused lifecycle/resource test under the loaded daemon workflow
  on Linux and retain the local macOS diagnostic recipe. The campaign must
  prove counters and OS evidence return to baseline after reconnect churn.

### Repository gates

Run targeted tests first, then:

```sh
cargo fmt --all --check
./test.sh -p botster-hub-client
./test.sh -p botster-hub-test-support
./test.sh --test hub_daemon_lifecycle_test
./test.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
node packages/hub-test-support/test.mjs
script/run-loaded-daemon-lifecycle-selftest
script/run-loaded-daemon-lifecycle \
  --subject-dir "$PWD" \
  --artifact-dir /tmp/botster-hub-connection-lifecycle \
  --subject-sha "$(git rev-parse HEAD)" \
  --test-target focused-connection-lifecycle \
  --repetitions 20 \
  --stress-profile residual-tail
```

Also run generated-asset check/sync commands documented by
`packages/hub-test-support` if public DTOs or fixtures change. Review the final
diff for legacy paths, unbounded channels/tasks, polling intervals,
unreachable counters, debug payload leakage, and implementation that exists
without being called by `serve_daemon`.

The implementation must add `focused-connection-lifecycle` to the runner and
workflow selector before using the command above. Both loaded-lifecycle scripts
are Linux harnesses: the selftest validates ownership/cleanup mechanics and is
not the macOS runtime recipe. The macOS proof must be captured separately with
the focused real-daemon test, public counter snapshots, `ps` thread/CPU
observations, and independently awaited child shutdown.

## Vault gaps worth capturing

- Capture the final invariant that one admitted daemon connection owns all of
  its terminal/entity registrations and cleanup is a once-only owner command,
  including the safe high-priority cleanup queue bound.
- Capture the Hub policy distinction between healthy indefinitely idle streams
  and deadlines for incomplete/stalled/dead transports.
- Capture the shared CoreDaemon reconciliation backoff rule and measured idle
  wake threshold, including cursor-change steady state, resync-only full
  baselines, and both subscriber/session scaling axes, because the current Core
  lifecycle journal has no blocking notification seam.
- Capture the exact reconnect counter definition once runtime evidence proves
  it, so future Web/TUI work does not reinterpret one-shot control connections
  as reconnects.
- Capture any cross-repository Core notification prerequisite if the Hub-only
  shared backstop cannot satisfy the measured idle threshold.
