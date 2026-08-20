# Implement: expose client event subscriptions on the host control protocol

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663583_640263`
- Run: `run_1787158085_722550`
- Implement step: `run_step_1787179103_419699` (sequence 17, seventh Review return)
- Prior Implement steps: `run_step_1787177596_331327`, `run_step_1787176520_385387`, `run_step_1787173421_386472`, `run_step_1787170885_485950`, `run_step_1787169048_973962`, `run_step_1787165407_410166`, `run_step_1787159472_279191`
- Approved plan: `docs/plans/expose-client-event-subscriptions-on-the-host-control-protocol.md` at `98ae1f9`
- Plan Review: `review_1787159455_211294` approved
- Code Review: `review_1787165393_430946` `changes_required` (7 findings, resolved)
- Code Review: `review_1787169037_550837` `changes_required` (4 findings, resolved)
- Code Review: `review_1787170872_296170` `changes_required` (3 findings, resolved)
- Code Review: `review_1787173413_659341` `changes_required` (2 findings, resolved)
- Code Review: `review_1787176510_516746` `changes_required` (1 finding, resolved)
- Code Review: `review_1787177586_705734` `changes_required` (1 finding, resolved)
- Code Review: `review_1787179092_801191` `changes_required` (1 finding)
- `teardown_class_applies`: no
- Direct-merge pipeline. No pull request.

`BOTSTER_TARGET_ID` and spawn-target routing both map
`tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. This run
used the pipeline-provided ticket worktree for that target.

## Review-return findings

Review `review_1787165393_430946` returned seven findings. This visit
addresses all of them. No protocol or npm coordinate change.

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787165393_643969` Unix client cannot wait for events | high | `DaemonConnection::next_event` plus `set_read_timeout`. Live Unix PackageEvent now arrives without a Status poll. |
| `finding_1787165393_273836` Event output starves control | high | Unix select is biased toward readable requests. Each flush turn writes at most three host frames (one visit per class). WebRTC treats queued Request/Hello/Overflow as `control_ready` and continues flushing a held entity frame. |
| `finding_1787165393_194973` Mailbox contention loses EventGap | high | Gap bits live on a separate mutex from the event queue. Lock-held ablation proves one EventGap and no replay. |
| `finding_1787165393_965740` Default-concurrency suite failed | high | `drive_package_events_for_test` drained one 20ms pull and reused `package-event-test-{name}-{envelope_id}` for two handlers of one envelope. Request ids now include `handler_id`. The helper loops queued copies and retries `Backpressured`. |
| `finding_1787165393_413502` Cleanup retain drops new IDs | medium | Commit removes only snapshot IDs that this pass completed. Concurrent-insert test covers the window. |
| `finding_1787165393_534534` Client delivery before plugin reject | medium | Clients receive only on `Accepted`. Mixed plugins-plus-clients pressure test covers `ShedFull` and `RejectedOverFanout`. |
| `finding_1787165393_221205` Live Unix EventGap missing | medium | IsolatedHub stall-file latch plus 1-slot mailbox. Proves one EventGap, no later traffic, Status during stall and after. |

Second Review return (`review_1787169037_550837`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787169037_297118` Gap-lock can still drop EventGap | high | Each subscription owns an `Arc<AtomicBool>` gap flag. Shed sets the bit without the slot-map lock. Slot-held ablation proves one EventGap. |
| `finding_1787169037_708275` Strict workspace Clippy fails | high | Removed the never-loop and Copy clones. `cargo clippy --workspace --all-targets --locked -- -D warnings` is clean. |
| `finding_1787169037_872794` Lua helper retries and can orphan | high | Helper now waits via `invoke`, one causal scope per copy, and requeues or retires on every exit. Ablation holds admit backpressure through the old 2s deadline and proves occupancy is unchanged. |
| `finding_1787169037_962336` Stall latch works outside test mode | medium | Stall file and queue-max override require `BOTSTER_ENV=test`. Negative unit tests cover production and unset env. |

Third Review return (`review_1787170872_296170`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787170872_438008` Invisible gap flag | high | `register_gap_slot` returns `ShedBusy` instead of an unmapped bit. Subscribe does not admit until the mailbox can find the bit. Lock-held admission test plus a real ingress shed proves EventGap with no manual `bit.store`. |
| `finding_1787170872_469595` Busy cleanup drops pulled copies | high | Requeue and complete return the delivery on busy. The test helper keeps a bounded retry store for deliveries and causal-scope releases. Lock-held tests prove occupancy is unchanged until the retry succeeds. |
| `finding_1787170872_111770` Public unbounded requeue | medium | `requeue_delivery` is crate-private. `ReadyDelivery` is not `Clone`. Each pull has a one-time `pull_id`. Duplicate requeue is rejected. Consumer queue bounds apply on requeue. |

Fourth Review return (`review_1787173413_659341`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787173413_130589` Production never consumes pull tokens | high | `EventDeliveryFlight` carries the pull token. Every owner-loop exit calls `retire_pulled`. Tests cover missing handler, admission rejection, causal-mint failure, and queued retirement. Outstanding pulls return to zero. |
| `finding_1787173413_737265` Helper drops leftover after 64 busy retries | high | Unsettled deliveries stay on the runtime. A later drive restores them. An ablation parks copies, holds the router lock through settle, then recovers occupancy. |

Fifth Review return (`review_1787176510_516746`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787176510_355069` Fan-out overwrites in-flight ownership | high | Production request IDs include `pull_id`. Two subscribers of one envelope keep two flights. Both completions retire the correct holder, release both causal scopes, and return occupancy to zero. |

Sixth Review return (`review_1787177586_705734`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787177586_367843` Fan-out tests bypass queued completion | high | Load the real event-probe Lua plugin with two `worktree_created` handlers. The production `PackageEventDelivery` slice queues both Core jobs. `CompletionDrain` retires both flights. Request IDs differ. Occupancy and outstanding pulls return to zero. |

Seventh Review return (`review_1787179092_801191`):

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787179092_749617` Test proof expands production Rust API | high | Removed the public owner-loop re-exports. Restored `#[cfg(test)]` on `test_outstanding_pulls`. Moved the real two-handler Core queue and drain proof into `daemon_maintenance` unit tests. |

Constraints applied for this return: Hub host-control and in-repo
`botster-hub-client` only. Core pin unchanged. No Web/TUI edits. No
protocol bump.

## Repository playbook and other playbooks/notes applied

Role:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]

Repository charters:

- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] (in-repo client crate)

Vault notes that constrained the change:

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[exact owner plus name is the only package event subscription key]]
- [[Package-event subject filters are exact strings compiled at admission]]
- [[Client event holders are connection-scoped]]
- [[Client event subscriptions stay on the multiplexed host-control path]]
- [[Fair host-control writing selects already-admitted frames]]
- [[Host package-event negotiation survives terminal admission rejection]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[Unix mux host events are unsolicited control frames]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[Unix mux host frames flush before new terminal slots]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[hub generated protocol changes are a four site release chain]]
- [[generated typescript dtos must encode serde field optionality]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub test support npm releases need external consumer smoke]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]

Convention conflicts: none.

Checklist: reused `checklist_1786867714_922206` as required.

## Files changed

Seventh Review return (this visit):

- `src/lib.rs` — no public owner-loop re-exports
- `src/package_event_router.rs` — `test_outstanding_pulls` is test-only again
- `src/daemon_maintenance.rs` — crate-private two-handler Core queue and drain proof
- `tests/hub_lua_runtime_test.rs` — removed the public-API owner-loop test
- `docs/reports/expose-client-event-subscriptions-on-the-host-control-protocol-implement.md`

Prior Review-return files remain:

- `src/daemon_maintenance.rs` — per-delivery production request IDs; fan-out occupancy proof
- `src/package_event_router.rs` — `retire_pulled` consumes the pull token
- `src/runtime.rs` — durable leftover store; never drop after settle-turn limit
- `tests/hub_lua_runtime_test.rs` — busy-settle occupancy ablation
- `src/daemon_event_subscriptions.rs` — gap-slot registration fails closed

- `crates/botster-hub-client/src/lib.rs` — `next_event` / `set_read_timeout`
- `src/daemon_transport.rs` — biased request poll, bounded flush, stall latch
- `src/host_control_fair_write.rs` — `MAX_HOST_FRAMES_PER_FLUSH_TURN`
- `src/local_webrtc.rs` — queued-request `control_ready`, held-entity continue
- `tests/hub_daemon_lifecycle/package_event_plane.rs` — next_event + Unix EventGap
- `tests/hub_lua_runtime_test.rs` — backpressure occupancy ablation
- `docs/client-protocol.md` — next_event

Prior Implement files remain:

Public contract:

- `crates/botster-hub-client/src/lib.rs` — `SubscribeEvents` /
  `UnsubscribeEvents`, `EventSubscribed` / `EventUnsubscribed`,
  `PackageEvent` / `EventGap`, `FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS`,
  `for_package_event_subscriptions()`, connection helper, mux classification,
  conformance revision 44
- `crates/botster-hub-client/src/typescript.rs` — generated DTO emission
- `crates/botster-hub-client/generated/daemon-protocol.ts` — `subjects?`,
  `subscribe_events`, `unsubscribe_events`, `package_event`, `event_gap`

Hub host control:

- `src/daemon_event_subscriptions.rs` — connection-scoped holders, subject
  admission ceilings, mailboxes, gap bits, coalesced wake, cleanup retry
- `src/host_control_fair_write.rs` — ready-set selection among control,
  entity, and event
- `src/package_event_router.rs` — client holders, compiled subject match,
  non-blocking client fanout
- `src/daemon_transport.rs` — host compatibility record, Unix mux Subscribe
  path, fair write, connection cleanup, owner-loop cleanup retry
- `src/local_webrtc.rs` — multiplexed Subscribe path, fair `try_recv`,
  EventGap live harness
- `src/lib.rs`, `src/main.rs` — module wiring and event display
- `src/unix_terminal_adapter.rs`, `src/webrtc_terminal_adapter.rs` — pending
  event readiness only; no adapter API calls on the event flood path

Release and proof:

- `packages/hub-test-support/*` — unpublished `0.1.39` at revision 44
- `examples/event-plane-producer/botster-package.json` — `clients` audience
- `docs/client-protocol.md`
- `tests/hub_daemon_lifecycle/package_event_plane.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`

## Ownership boundaries preserved

Hub owns host-control admission, connection-scoped event egress, and fair
host-control writing. The in-repo `botster-hub-client` crate owns public DTOs
and compatibility descriptors.

Core pin stays
`https://github.com/trybotster/botster-core.git`
rev `8fce2041b9fe742cb2a6df9e74cb262606672742`. This ticket does not change
Core.

Terminal bytes stay in SessionIo and ClientWorker. Package events do not
enter the exclusive Unix entity socket. Occupancy, Unix admission acks,
adapter close-work, owner-loop background scheduling, snapshot paging, and
ShutdownSession exact-generation suppression remain from merged blockers.

No Workspaces or Project Pipelines product event names in Hub.

## Cross-repo dependencies or separately routed work

Registered downstream consumers stay separate:

- `ticket_1786663584_427840` botster-web
- `ticket_1786663585_944018` botster-tui

This run does not vendor into those repositories. TUI scratch cargo-patch
compile against this `botster-hub-client` succeeded with zero source
breakage (TUI SHA `dc7d600`). Web scratch install of packed `0.1.39` typechecks.
`npm test` reports expected vendored-protocol drift; that is the Web ticket's
site-4 work.

No new Core, Web, TUI, or Workspaces dependency.

## Deviations from plan

None in product scope. Replay resolved conflicts by keeping both occupancy
and package-event behavior.

Release coordinate: orphan `0.1.38` stays untouched. This run published
`@trybotster/hub-test-support@0.1.39` at conformance revision 44.

Registry `dist.integrity` matches the packed tarball from HEAD `3f3e55d`:
`sha512-r7gnVD+uiGvoeSzRnrgIM1+ZCjrRZUOlJz7x0bPYMkn3pF1kjJSIVAV1fEiPjqERv4eGtHYV1xgbjGGBVZP2Yg==`
(`dist.shasum` `03f47f651e272a52ebf2e6a5e2bea2e2fe7b6851`).

A clean install of the published coordinate proved `metadata.package_version
=== "0.1.39"`, revision 44, protocol 7, `verifyPackageAssets()` empty
failures, DTO tokens `subscribe_events` / `unsubscribe_events` /
`package_event` / `event_gap` / `subjects?`, and
`package_event_subscriptions` in `supported_features` only.

## Tests and downstream proof run

Live IsolatedHub production path:

- Unix negotiated `SubscribeEvents` delivers unsolicited `PackageEvent`
- Unix unnegotiated subscribe is a typed error
- Unix Hello that fails terminal compatibility still Status + SubscribeEvents
- Unix reconnect does not replay
- Unix subject/audience admission returns typed operator errors
- WebRTC IsolatedHub delivers unsolicited `PackageEvent` on a negotiated
  DataChannel

Unit and mux:

- subject UTF-8 ceilings, 64/65 cap, connection-scoped holders, gap-first
  write, missed-notify wake bit, cleanup retry
- fair ready-set among control/entity/event
- DTO omit replay/empty subjects; operation-specific requirement
- WebRTC harness EventGap after a full mailbox
- Unix partial `PackageEvent` line resumes without interleaving

Merged-blocker named tests still pass:

- `near_limit_snapshot_assembly_stays_within_owner_turn`
- `snapshot_page_charges_the_real_envelope_not_a_stub`
- `shutdown_session_arm_installs_exact_suppression_before_core_request`
- `close_event_suppression_matrix_matches_prior_predicate`

Repo gates:

- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `./test.sh --locked` — one clean default-concurrency pass after the
  seventh Review-return fixes, 0 failures. Lifecycle 264 passed / 1 ignored.
  Lua runtime 62 passed. Hub lib 453 passed, including the crate-private
  owner-loop two-handler Core queue and completion drain. Hub client 78
  passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` clean.
- `cargo fmt --all -- --check`
- `cargo test --doc --workspace`
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
- `node packages/hub-test-support/test.mjs`
- `npm install @trybotster/hub-test-support@0.1.39` in a clean directory,
  then `verifyPackageAssets()` plus DTO-token and support-matrix asserts

Live provenance under this checkout:

- Hub SHA after the product replay commits: `4aeec2f`
- Version-bump commit follows this report
- Locked Core SHA `8fce2041b9fe742cb2a6df9e74cb262606672742`
- Hub binary realpath is the ticket worktree `target/debug/botster-hub`

## Unverified behavior or residual risk

- `@trybotster/hub-test-support@0.1.39` is published and clean-install
  proven. Web and TUI still need their own site-4 vendor/pin tickets.
- Unix EventGap live proof uses a test-only stall file
  (`BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH`) and a 1-slot mailbox
  (`BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX`). That is a controlled write
  stall, not a production config surface.
- Ready-set order treats WebRTC close events and package events as one Event
  class, as the plan allowed.
- Flush turns are bounded at three already-admitted host frames. Continuous
  event ingress can still occupy later turns; the bound exists so control
  and entity classes get a select/poll between turns.

## Missing vault guidance discovered

Captured after replay proof:

- An unmerged run that publishes an npm coordinate burns it.
- A closed run's implement commits can dangle with no branch; recover the
  tip and protect it with a `keep/<ticket>` tag before replay.

Inbox files:

- `an-unmerged-run-that-publishes-an-npm-coordinate-burns-it.md`
- `closed-run-implement-commits-can-dangle-without-a-branch.md`
