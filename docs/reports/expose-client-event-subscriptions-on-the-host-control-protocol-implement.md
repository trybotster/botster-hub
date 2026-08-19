# Implement: expose client event subscriptions on the host control protocol

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663583_640263`
- Run: `run_1787158085_722550`
- Implement step: `run_step_1787159472_279191`
- Approved plan: `docs/plans/expose-client-event-subscriptions-on-the-host-control-protocol.md` at `98ae1f9`
- Plan Review: `review_1787159455_211294` approved
- `teardown_class_applies`: no
- Direct-merge pipeline. No pull request.

`BOTSTER_TARGET_ID` and spawn-target routing both map
`tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. This run
used the pipeline-provided ticket worktree for that target.

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

Checklist: reused `checklist_1786867714_922206` as required. Botster MCP was
down in this agent session, so Implement evidence is also in this report and
gate payload.

## Files changed

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

Release coordinate: registry latest remains orphan `0.1.38`. This run takes
unpublished `0.1.39` / conformance revision 44 as planned.

`npm whoami` is 401 and `npm publish` returns 404/unauthorized. Packed
tarball `trybotster-hub-test-support-0.1.39.tgz` (shasum
`03f47f651e272a52ebf2e6a5e2bea2e2fe7b6851`) contains revision 44,
`package_event_subscriptions` in `supported_features` only, and the new DTO
tokens. Publication needs restored npm auth, same as prior
`question_1786874473_381921`.

Botster MCP handshake failed in this agent session. Workflow evidence is
recorded here and will be persisted through Project Pipelines tools when
available.

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
- `./test.sh --locked` — one clean default-concurrency pass, 0 failures
  (lifecycle 263 passed / 1 ignored)
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --doc --workspace`
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
- `node packages/hub-test-support/test.mjs`

Live provenance under this checkout:

- Hub SHA after the product replay commits: `4aeec2f`
- Version-bump commit follows this report
- Locked Core SHA `8fce2041b9fe742cb2a6df9e74cb262606672742`
- Hub binary realpath is the ticket worktree `target/debug/botster-hub`

## Unverified behavior or residual risk

- `@trybotster/hub-test-support@0.1.39` is packed and locally proven, not
  published. Web and TUI cannot consume a registry coordinate until npm auth
  is restored.
- IsolatedHub Unix EventGap under a live emit flood is not a reliable fill
  because the writer can drain the 1-slot mailbox between emits. EventGap is
  proved on the WebRTC harness and in unit tests.
- Ready-set order treats WebRTC close events and package events as one Event
  class, as the plan allowed.

## Missing vault guidance discovered

Captured after replay proof:

- An unmerged run that publishes an npm coordinate burns it.
- A closed run's implement commits can dangle with no branch; recover the
  tip and protect it with a `keep/<ticket>` tag before replay.

Inbox files:

- `an-unmerged-run-that-publishes-an-npm-coordinate-burns-it.md`
- `closed-run-implement-commits-can-dangle-without-a-branch.md`
