# Eliminate unbounded polling connection and entity-subscription lifecycles

## Target

- Repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Run: `run_1785199801_176415`
- Implement steps: `run_step_1785200938_377318` and
  `run_step_1785212561_440065`

The approved plan and the authoritative Project Pipelines target both route
this work to `botster-hub`.

## Guidance applied

- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]].
- Repository charter: [[botster-hub-playbook]].
- Changed-surface overlay: [[botster-runtime-reviewer-playbook]].
- Workflow discipline: [[project-pipelines-playbook]].
- Hub ownership and transport notes:
  [[botster hub is a first party host profile over core]],
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
- Client/reconnect notes:
  [[botster entity snapshots are authoritative reconnect baselines]],
  [[botster client subscriptions should not hydrate global state]],
  [[subscription ID namespacing separates TUI and browser clients]],
  [[attach reconnect must drop stale outbound requests before resubscribe]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[adding a hub client feature constant is a three site change]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  and [[generated typescript dtos must encode serde field optionality]].

No loaded convention conflicted with the approved plan.

## Implementation

The production `serve_daemon` path now owns a fixed two-worker Tokio transport
reactor. Unix accepts, held-open reads, entity delivery, cancellation, and
bounded writes use async waits rather than one detached OS thread and one
20 ms poll loop per client. Admission is capped at 64 live connections, the
owner control queue is a bounded Tokio channel, replies use one-shot channels,
and excess clients receive a typed backpressure handshake without entering
request execution. Active requests and WebRTC control delivery therefore
neither park reactor workers on blocking sends nor allocate a blocking-pool
thread per request.

Every admitted connection creates its cleanup guard before the handshake. The
guard owns attach and entity registrations and emits one cleanup record for
EOF, protocol/incomplete-frame failure, stalled write, cancellation, shutdown,
or normal close. The owner performs detach/unsubscribe and publishes sanitized
current, high-water, cleanup, reconnect, reconciliation, delivery, and stalled
write counters through `DaemonStatus`. Reconnect accounting consumes bounded
released-generation credits, so protocol-compliant fresh subscription ids are
counted without retaining connection or subscription history.

Lifecycle reconciliation is seeded once before the listener begins accepting
clients. Subscribers use the cached authoritative baseline; steady state reads
the Core lifecycle change journal at one shared 500 ms cadence and drains only
remembered sessions. A filesystem-backed baseline is read again only when Core
reports `resync_required`. This keeps potentially slow initial registry I/O
outside the admitted-client request path.

Shutdown signals and joins connection tasks before stopping the daemon and
unlinking the listener. The `down` command verifies owned daemon metadata and
waits for that process to exit before returning, so immediate `up` cannot
overlap old session or entrypoint teardown.

## Files changed

- Runtime and policy: `src/daemon_transport.rs`, `src/local_webrtc.rs`,
  `src/main.rs`, `Cargo.toml`, and `Cargo.lock`.
- Public client contract: `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and the generated TypeScript
  artifacts.
- Downstream-shaped support: `packages/hub-test-support` generated assets,
  metadata, support matrix, and package test.
- Proof and CI: `tests/hub_daemon_lifecycle_test.rs`,
  `script/run-loaded-daemon-lifecycle`, and
  `.github/workflows/loaded-daemon-lifecycle.yml`.
- Documentation: `README.md`, `docs/client-protocol.md`, the approved plan,
  and this report.

## Ownership boundaries and cross-repository work

The change remains inside Hub transport/runtime policy and the in-repository
Hub client/test-support public seams. Core remains the policy-free lifecycle
journal and terminal mechanism; terminal bytes still bypass Hub through the
existing session/client actors. No Web, TUI, Core, legacy-monolith, Project
Pipelines plugin, package-admission, or supervision policy code changed.

No new cross-repository dependency or separately routed implementation work
was required. The existing closed UiNode dependency remains unrelated.

## Deviations from the approved plan

- The initial lifecycle baseline is seeded during daemon startup rather than
  on the first subscription. This is a stricter realization of the approved
  single-baseline design: a slow registry cannot block an already admitted
  unrelated client, and the first live subscriber is proven not to perform
  owner-path baseline I/O.
- The shared natural-exit backstop uses a fixed 500 ms cadence rather than an
  adaptive cadence. Counter proof bounds it to at most four wakes/change reads
  in a 1.1 second observation window and proves the cadence is independent of
  both subscriber and session count.
- The local deterministic matrix uses eight subscribers and eight live
  sessions rather than the separate four-package fixture. Package count does
  not participate in the lifecycle journal algorithm; the Linux loaded
  campaign remains the production contention proof.
- Review removed the proposed `cleanup_already_complete` field because the
  single-owner cleanup guard has no legitimate duplicate producer. Cleanup
  enqueue failure degrades to a diagnostic rather than panicking in `Drop`.

## Verification and downstream proof

Passed:

- `cargo fmt --all --check`
- `./test.sh --workspace --no-run`
- `./test.sh -p botster-hub-client` — 42 unit tests and 4 doctests
- `./test.sh -p botster-hub-test-support` — 32 unit tests and 3 doctests
- `./test.sh -p botster-hub --lib` — 112 tests
- `./test.sh` — full Hub suite; 101 lifecycle integration tests passed and the
  one documented large local adversarial test remained ignored
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `node packages/hub-test-support/test.mjs`
- generated package sync via
  `node packages/hub-test-support/scripts/sync-assets.mjs`
- focused production-binary proof:
  `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible`
- immediate down/up regression proof:
  `cli_local_runtime_up_starts_reuses_and_down_stops_runtime`
- loaded campaign input validation for
  `focused-connection-lifecycle`, 20 repetitions, `residual-tail`

The focused production test proves:

- one startup baseline and zero live-subscription baseline reads;
- no timer-driven entity delivery;
- at most four shared reconciliation wakes/change reads per 1.1 seconds with
  eight subscribers and eight live sessions;
- 64 live idle connections with high-water 64 and bounded OS thread growth;
- typed admission rejection while an admitted client remains responsive;
- cleanup convergence after abrupt drops;
- sixteen repeated fresh-id subscribe/drop reconnect generations with live
  entity subscriptions returning to zero and high-water remaining bounded;
- live/high-water attach counters plus a real failed cleanup after its session
  is removed;
- accepted-connection, delivery overflow/failure, and stalled-write producers;
- deadlines and cleanup for half-open handshake, malformed complete frame, and
  incomplete frame;
- clean daemon shutdown with live idle/entity clients, followed immediately by
  a non-overlapping replacement daemon.

Existing Hub tests continue to cover failed response/entity delivery, duplicate
attach/entity identifiers, slow terminal consumers, fresh authoritative entity
snapshots after reconnect, local WebRTC cleanup-once behavior, and shutdown
response-delivery ordering.

## Unverified behavior and residual risk

- The Linux-only loaded lifecycle selftest and 20-repetition campaign were not
  executed on macOS because the harness requires `setsid`, `/proc`, and Linux
  `ps` behavior. The selector and arguments validate locally and the workflow
  exposes the new target; CI must retain the campaign artifact.
- Local proof uses deterministic wake/delivery counters and process thread
  count. Exact process CPU/context-switch sampling remains a loaded Linux
  campaign responsibility because macOS `ps` CPU time is too coarse for the
  short idle window.
- The startup seed structurally removes slow baseline I/O from the live
  request path and is counter-tested, but no artificial filesystem-delay hook
  was added solely for a timing test.
- Admission/control constants are fixed Hub policy. They are intentionally not
  configurable in this ticket.

## Missing vault guidance and durable capture

Four reusable invariants were confirmed:

- an admitted daemon connection owns all of its attach/entity registrations
  and has one cleanup record;
- healthy frame-complete idle streams have no lease, while incomplete,
  stalled, cancelled, and dead transports have deadlines;
- Hub seeds one lifecycle baseline before admission and uses cursor changes in
  steady state, with full baselines only after explicit Core resync;
- reconnect counts subscription generations, not one-shot control requests.

These are missing as concise atomic vault notes. They were not written from
this repository run because the vault is outside the routed repository
ownership/write boundary. Durable capture should be registered as separate
knowledge-vault work rather than silently broadening this implementation run.

## Assumptions

- A two-worker Tokio reactor and the fixed caps are Hub-owned policy, not user
  configuration.
- Core's ordered lifecycle journal remains the authoritative steady-state seam;
  `resync_required` is the only reason to repeat the filesystem baseline.
- Existing Web/TUI consumers tolerate an optional status field because Rust
  defaults it for older daemon JSON and generated TypeScript marks it optional.
- Aggregate released-generation credits are sufficient for the requested
  reconnect counter; client identity is intentionally not inferred from
  transport-local subscription ids.
