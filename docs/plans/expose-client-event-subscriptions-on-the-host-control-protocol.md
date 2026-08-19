# Hub: expose client event subscriptions on the host control protocol

## Run 2 re-entry (run_1787158085_722550)

This is a fresh run of the same ticket after prior run
`run_1786867268_135671` closed mid-Implement. The prior run produced:

- An approved plan. Plan Review `review_1786869451_505272` approved
  it after two `changes_required` rounds. Human answer
  `question_1786868431_577955` fixed the subject ceilings. Both
  decisions stay in force.
- A complete implementation with review-return fixes:
  commits `65e66e9`, `7d4d0ab`, `9b4b8bc`, `a16c413`, `22ec8b6`,
  `4b252e0`, based on old main `60b79b8`.
- A published `@trybotster/hub-test-support@0.1.38` (now an orphan;
  see Release coordinates below).

The prior run blocked on full-suite nondeterminism. Human answer
`question_1786875726_281871` required blocker tickets instead of a
waiver. Those blockers are now closed and merged into main:
owner-loop background scheduling (`ticket_1786912569_840742`),
deterministic snapshot paging (`ticket_1786912570_127968`), PTY
process and marker oracles (`ticket_1786912572_610381`), plus the
ShutdownSession close-event suppression ticket. That answer still
requires one clean default-concurrency `./test.sh --locked` on the
refreshed base. This plan carries that requirement forward.

### Recovery and replay strategy

The prior implementation commits are dangling (no branch holds
them). Tag `keep/ticket_1786663583_640263-prior-tip` now protects
tip `4b252e0` from garbage collection.

Implement must replay the approved implementation onto the new base
`122d65a` instead of re-inventing it:

1. Cherry-pick `65e66e9`, `7d4d0ab`, `9b4b8bc`, `22ec8b6` in order
   (the two report-only commits `a16c413` and `4b252e0` fold into a
   fresh implement report).
2. Resolve conflicts toward the approved design in this document.
   Expect heavy conflicts: main added +1,377 lines to
   `src/daemon_transport.rs`, reworked `src/local_webrtc.rs`, and
   rewrote the test harness (`tests/hub_daemon_lifecycle/harness.rs`,
   new `harness_isolation.rs`, guard idioms) since `60b79b8`.
3. Renumber release coordinates (below) and re-run every gate on
   the new base. Prior green evidence does not carry over.

### Release coordinates on the new base

Main moved while the prior work was in flight:

- Main is now `CONFORMANCE_FIXTURE_REVISION = 43` at unpublished
  `hub-test-support` `0.1.37` (Unix attach occupancy, `e7c0e7e`).
- The registry latest is `0.1.38`, published by the prior run. It
  claims revision 43 with the event DTOs but without the occupancy
  additions. Its content no longer matches any tree. Treat `0.1.38`
  as a burned orphan coordinate. Never republish or mutate it.
- This replay therefore bumps revision 43 -> 44 and takes version
  `0.1.39` (the next unused registry coordinate). The published
  `0.1.39` at revision 44 contains both the occupancy baseline and
  the event-subscription DTOs and supersedes the orphan.

### Merged-blocker integration constraints

- Owner-loop background scheduling: package-event ingress and
  client egress stay off the bounded background scheduler.
  `try_ingress` remains non-blocking router ingress
  ([[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]).
  The replay must not route event mailbox writes through Pump or
  Maintenance turns.
- ShutdownSession suppression: suppression covers exact terminal
  route generations. It must not suppress or reorder package-event
  host frames. The replay must not add package events to the
  suppression snapshot.
- Deterministic snapshot paging: fair host-control writing treats
  entity snapshot pages as already-admitted entity frames. The
  replay must not re-encode or re-page snapshots in the fair
  writer.
- Test harness: port the prior ticket tests to the current harness
  idioms (`daemon_test_guard` at real-daemon start boundaries,
  IsolatedHub isolation, decision-level oracles instead of
  wall-clock waits).

This visit reuses vault checklist `checklist_1786867714_922206`
with a new re-entry item. Do not create a second checklist.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Spawn-target name: `botster-hub`.
- Authoritative target path comes from `list_spawn_targets`, not the
  ambient process working directory.
- Pipeline ticket: `ticket_1786663583_640263`.
- Run: `run_1787158085_722550` (prior run `run_1786867268_135671`
  is closed).
- Project: Botster Non-Blocking Event Plane, Stage C Hub slice.
- Assigned worktree is the pipeline-created Hub worktree for this
  ticket.
- Prior-run plan commits `24da5a0`, `b0d7393`, `6f1f5cf` live on
  the preserved tag, not on main.
- `origin/main` at this visit: `122d65a`. The branch equals main.
- Required Core pin, exact, all Git-visible members:
  `https://github.com/trybotster/botster-core.git`
  rev `8fce2041b9fe742cb2a6df9e74cb262606672742` (current main
  pin; it replaced the prior run's `fc541a59`).
  Do not float `branch = "main"`. This ticket does not need a new
  Core pin.
- `teardown_class_applies`: no.
- This is not a Hub session-type eligibility consumer. Do not inject
  `list_session_types_for_target` parent pins.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role overlays:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Surface charter (ticket names `botster-hub-client`, which lives in
this Hub repo):

- [[botster-hub-client-playbook]]

Planner must-load maps and orchestration notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[cross repo dependency registration must use dependency repo target]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[current botster is a modular repository family not the legacy trybotster monorepo]]

Workflow charter (this run uses direct-merge, artifacts, gates, and
checklists):

- [[project-pipelines-playbook]]
- [[plan steps need reviewable plan artifacts]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[plan review routes process and infrastructure findings without full replanning]]
- [[verification evidence is scoped to a stable commit and clean tree]]
- [[pipeline run worktrees allow only one active writer]]
- [[project pipelines mcp create calls can time out after committing]]
- [[implement gate must verify committed work and pr link before review]]

Hub and client notes implicated by this ticket:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[exact owner plus name is the only package event subscription key]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[package event contracts live on HubPackageManifest not Core PackageManifest]]
- [[admitted event holders survive producer unload until Core completion]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Unix mux host events are unsolicited control frames]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[Unix mux host frames flush before new terminal slots]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub generated protocol changes are a four site release chain]]
- [[generated typescript dtos must encode serde field optionality]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[botster packages should enforce core hub cli plugin provider boundaries]]

Process notes:

- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Intentionally not loaded:

- [[botster runtime teardown lenses]] — this ticket does not change
  WebRTC or SessionIo or ClientWorker teardown, multi-peer ownership,
  CPU or FD spin, or terminal-state versus live-runtime divergence.
- [[botster-core-playbook]] — Core is a closed pin, not this ticket's
  ownership charter.
- [[botster-web-playbook]] and [[botster-tui-playbook]] — those are
  registered downstream consumers, not this run.
- [[project-pipelines-playbook]] package/plugin paths — this ticket
  does not change Project Pipelines package source.

## Context loaded

Stage B already landed on this HEAD (`122d65a`):

- `src/package_event_router.rs` is Send-safe. It owns contracts,
  exact plugin subscriptions, token buckets, occupancy, and
  transient queues. Every API uses `try_lock`. Contention is
  `shed_busy`.
- `try_ingress` fans out only to plugin holders whose contract
  audience contains `plugins`. Audience `clients` may already be
  stored. It is not delivered.
- Built-in owner `hub` emits worktree events through `try_ingress`.
  `DaemonEvent::WorktreeLifecycle` remains on the mutating response.
  That is request-scoped host control, not Stage C delivery.
- Live IsolatedHub proof exists in
  `tests/hub_daemon_lifecycle/package_event_plane.rs`.

Current host-control write paths:

- Unix exclusive entity subscribe (`handle_entity_subscription_async`)
  takes over the socket. After accept, the socket accepts only the
  matching `UnsubscribeEntities`. Entity frames use a dedicated
  `tokio` mailbox (`ENTITY_SUBSCRIPTION_QUEUE_CAPACITY`).
- Unix mux host loop writes `PendingMuxClass::{Response, Event,
  Terminal}`. Host frames flush before a new terminal slot. Event
  here is unsolicited host control such as
  `TerminalSubscriptionClosed`, not package events.
- WebRTC `run_data_channel` already multiplexes control requests and
  entity frames on one DataChannel. Unsolicited host events use
  `DaemonLocalWebrtcDeliveryKind::DaemonEvent`.

Current protocol constants on `122d65a`:

- `PROTOCOL_VERSION = 7`
- `CONFORMANCE_FIXTURE_REVISION = 43` (Unix attach occupancy)
- `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION = 36`
- `@trybotster/hub-test-support` `0.1.37` in-tree, unpublished.
  Registry latest is the orphan `0.1.38` (see Release coordinates).
- `DaemonResponseKind::Events` already means attach or drain event
  batches. Do not reuse it for package-event subscribe.

Repo placement: Hub `docs/plans/` is living prior art on `main`.

Worktree hygiene: tracked `.gitignore` is present and non-empty
(53 bytes, matches HEAD). Worktree path has no `:`. No
`CARGO_TARGET_DIR` override is required for colon reasons.

## Scope

Expose transient package events to host-control clients without
entering the terminal transport plane.

### 1. Public host-control contract in `botster-hub-client`

Add request variants. They are ordinary one-shot host-control
requests. They must not take over the connection the way
`SubscribeEntities` does on Unix.

```text
DaemonRequest::SubscribeEvents {
    subscription_id: String,
    owner: String,
    name: String,
    subjects: Vec<String>,  // omit when empty
}
DaemonRequest::UnsubscribeEvents {
    subscription_id: String,
}
```

Add response kinds `EventSubscribed` and `EventUnsubscribed`.
Do not reuse `DaemonResponseKind::Events`.

Add unsolicited `DaemonEvent` variants:

```text
DaemonEvent::PackageEvent {
    subscription_id: String,
    owner: String,
    name: String,
    payload: Value,
}
DaemonEvent::EventGap {
    subscription_id: String,
    owner: String,
    name: String,
}
```

Public DTO rules:

- No event sequence, cursor, replay request, durable-history option,
  recovery-family field, or timestamp used as a replay key.
- `subjects` uses `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.
  Generated TypeScript must mark it optional.
- Do not add `#[non_exhaustive]` unless a scratch consumer probe
  proves that is the smaller break. Adding enum variants is already
  source-breaking for exhaustive matches.

Compatibility:

- Keep `PROTOCOL_VERSION` at 7.
- Bump `CONFORMANCE_FIXTURE_REVISION` from 43 to 44.
- Keep `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` at 36.
- Add `FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS`.
- Advertise it on `DaemonCompatibility::current()`.
- Do not add it to `DaemonCompatibilityRequirement::current()`.
- Add `DaemonCompatibilityRequirement::for_package_event_subscriptions()`
  that requires the feature and the new conformance floor.
- Classify `package_event` and `event_gap` in
  `parse_unix_mux_value` as `DaemonUnixMuxFrame::Event`.
- Emit those variants only to connections that currently hold the
  matching subscription.

Client helper:

- New connections that intend to subscribe must Hello with
  `DaemonCompatibilityRequirement::for_package_event_subscriptions()`.
- Add `hello_requires_package_event_subscriptions(required_features)`.
  It inspects host Hello `compatibility.required_features` only.
- A helper that opens a new connection uses that requirement.
- A helper on an existing `DaemonConnection` first checks that the
  negotiated host Hello already required the feature. If it did
  not, return a typed compatibility error and send no request.
- Do not add a held-open exclusive socket helper that copies
  `subscribe_entities`.
- One-shot `request()` remains valid only after a negotiated host
  Hello.

Hub enforcement, independent of terminal admission
([[Unix Hello can reject terminal admission while host operations remain available]],
[[public protocol versions host control and Core terminal planes independently]]):

- On every Hello, store a per-connection host compatibility record
  keyed by the same opaque connection id as event holders
  (Unix `client_id`, WebRTC `grant_id`). The record holds
  `hello.compatibility.required_features`.
- Write that record for both admitted and rejected terminal
  results. Current `UnixTerminalAdmission::Rejected` and
  `WebrtcTerminalAdmission::Rejected` store only a code and
  diagnostic. Do not read host event negotiation from those
  enums.
- Do not reuse `UnixTerminalAdmission::Admitted.required_features`
  or `WebrtcTerminalAdmission::Admitted.required_features` as the
  package-event gate. Those fields serve terminal-adapter
  negotiation, including close events.
- Gate `SubscribeEvents` and `PackageEvent` / `EventGap` delivery
  from the host compatibility record only.
- Reject `SubscribeEvents` when that host record does not contain
  `FEATURE_PACKAGE_EVENT_SUBSCRIPTIONS`. Return a typed operator
  or compatibility error. Do not drop the connection.
- Never emit `PackageEvent` or `EventGap` to a connection whose
  host record omitted the feature.
- A terminal compatibility rejection must not clear or hide a
  negotiated host feature.
- Classify `package_event` and `event_gap` in
  `parse_unix_mux_value` so a negotiated client can decode them.
  Unnegotiated clients must not receive those frames.
- Connection cleanup removes the host compatibility record with
  the connection.

### 2. Admit exact owner plus name, plus bounded subject filters

Subscription key remains exact `(owner, name)`
([[exact owner plus name is the only package event subscription key]]).

Subject and connection ceilings come from answered question
`question_1786868431_577955`. They are protocol admission ceilings,
not `PackageEventPlanePolicy` knobs.

- Maximum 16 subject values per subscription.
- Maximum 256 UTF-8 encoded bytes per subject value.
- Maximum 4,096 subject-value bytes per subscription.
- Maximum 64 active package-event subscriptions per host-control
  connection.

Admission rules:

- Empty `subjects` means every live event for that owner and name.
- Non-empty `subjects` compiles to an exact-match set at subscribe
  time. Ingress matches `payload.subject` against that set only.
  Do not scan or validate raw filter input on event ingress.
- Missing or non-string `payload.subject` does not match a
  non-empty compiled set.
- Reject empty values, wildcards, glob characters, duplicate
  values, more than 16 values, a value longer than 256 UTF-8 bytes,
  aggregate subject bytes above 4,096, and a 65th subscription on
  the same connection. Return typed operator errors.
- Count UTF-8 bytes, not Unicode scalars or UTF-16 units.

Admission failures for owner, name, and audience still return
typed operator errors. Map router statuses (`rejected_undeclared`,
`rejected_foreign`, `rejected_wildcard`, `rejected_audience`,
`shed_busy`) rather than dropping the client connection.

Deliver only when the stored contract audience contains `clients`.

### 3. Connection-scoped holders and bounded event egress

Add `src/daemon_event_subscriptions.rs`. Do not put package-event
frames on `EntityFrameSender`.

Opaque connection identity:

- Unix: the existing socket `client_id`
  (`botster-hub-daemon-socket-*` in `handle_connection_async`).
- WebRTC: the existing peer `grant_id`.
- Store that id on every client holder. Do not put it on public
  DTOs.
- Holder identity is `(connection_id, subscription_id)`.
- The same `subscription_id` on two connections creates two
  holders. They do not overwrite each other.
- A second `SubscribeEvents` with the same
  `(connection_id, subscription_id)` is a typed duplicate reject.
- `UnsubscribeEvents { subscription_id }` applies only to the
  calling connection. A foreign connection with the same id
  receives a typed miss and does not mutate the other holder.
- Connection cleanup drops only that connection's holders and
  mailboxes. A later subscribe on a new connection starts empty.

Per host-control connection:

- Bounded event mailbox (count and byte limits from existing
  `PackageEventPlanePolicy` consumer bounds).
- One gap bit per subscription, stored outside the mailbox.
- One coalesced writer wake per connection, stored outside the
  mailbox. Set it with an atomic or notify. Do not enqueue the wake
  as an event frame.

Router changes stay inside `src/package_event_router.rs`:

- Index client holders by exact `(owner, name)` plus
  `(connection_id, subscription_id)`.
- Do not reuse `EventSubscription.plugin_key` for clients.
- `try_ingress` already serializes one size-capped envelope. Extend
  the same non-blocking attempt to client holders when audience
  contains `clients`.
- Match subject filters only through the compiled exact set.
- If the mailbox is full or the copy has expired, do not enqueue
  the event. Set the subscription gap bit and the writer wake.
  Ingress still returns a typed result to the producer. Hub
  operations do not wait.
- Repeated sheds while the bit is set stay one bit. Do not queue
  history behind the gap.
- Keep plugin delivery and `try_admit(Background)` unchanged.

EventGap write path:

- The write loop treats a set gap bit as an already-admitted Event
  frame. It does not wait for a later live event.
- When a mailbox slot is free and the gap bit is set, write exactly
  one `EventGap`, then clear the bit.
- If a later live event is also admitted, write the gap first, then
  the live event. Never replay shed events.
- Prove the no-later-event case: fill the mailbox, shed, wait until
  the client drains enough to free a slot, and observe exactly one
  `EventGap` with no extra event.

Owner unload still uses `EventPlaneOwnerOps` and `try_apply`.
Client holders for an unloaded owner stop matching. Already
admitted plugin Background jobs stay under
[[admitted event holders survive producer unload until Core completion]].
Client mailboxes are not Core jobs.

### 4. Route only through the Hub host control protocol

Production entry points:

- Unix mux host loop: handle `SubscribeEvents` and
  `UnsubscribeEvents` as `ControlMessage::Request`. Keep the
  connection on the multiplexed host-control loop.
- WebRTC `run_data_channel`: same requests on the existing request
  arm.
- Deliver `PackageEvent` and `EventGap` as unsolicited host events
  only after Hello negotiated the feature: Unix
  `PendingMuxClass::Event`, WebRTC
  `DaemonLocalWebrtcDeliveryKind::DaemonEvent`.
- Route through `HubClientApi` / `HubRuntime`. Do not call raw Core
  routers.
- Pass the connection id into subscribe, unsubscribe, and cleanup
  control messages. Do not trust a client-supplied connection id.

Unix exclusive entity stream stays exclusive. That socket still
accepts only its matching `UnsubscribeEntities`. Package events do
not enter that mailbox.

### 5. Fair host-control writing

Fairness applies only to already-admitted host-control frames:
control responses, entity frames, and package-event frames. A set
gap bit counts as an already-admitted Event frame.

Rules:

- Inspect the three ready queues.
- Pick the next non-empty class after the last written class
  (round-robin).
- If only one class is ready, write it now.
- Never wait for a future slot in an empty class.
- Never inspect, schedule, queue, retry, translate, or branch on
  terminal adapter frames to make this choice.
- Do not implement terminal fairness.

Preserve existing terminal rules:

- Finish a partial terminal line with a nonzero offset before any
  host frame.
- Flush ready host frames before starting a new terminal slot
  ([[Unix mux host frames flush before new terminal slots]]).

Shared helper: add a small private module such as
`src/host_control_fair_write.rs` so Unix mux and WebRTC use the
same ready-set rule. Do not invent a new runtime crate.

WebRTC change: stop treating `entity_frame_rx.recv()` as a wait
that can starve control or events. Use `try_recv` on already
admitted entity and event frames, then wait only when no host-control
class is ready.

Unix mux change: replace the current "all responses, then all host
events, then terminal" drain with fair selection among already
queued Response and Event frames, then the existing terminal path.
Entity frames on the exclusive Unix entity socket stay on that
socket. WebRTC is the transport that must prove all three classes
on one connection.

### 6. Generated TypeScript, fixtures, and support package

Follow [[hub generated protocol changes are a four site release chain]]:

1. Update `crates/botster-hub-client/src/typescript.rs` and the
   checked-in `generated/daemon-protocol.ts`.
2. Bump `@trybotster/hub-test-support` to `0.1.39`, the next unused
   registry coordinate. `0.1.38` is the burned orphan from the
   prior run and `0.1.37` never published. Sync
   `daemon-protocol.ts`, `first-party-client-support-matrix.json`,
   and `metadata.json` at conformance revision 44.
   Put `package_event_subscriptions` in `supported_features` only.
3. Publish that coordinate from the synced tree and prove it with a
   clean installed-consumer smoke
   ([[hub test support npm releases need external consumer smoke]]).
   If `npm whoami` fails, ask the human to restore auth or publish,
   as in prior question `question_1786874473_381921`.
4. Do not vendor into Web or TUI in this run. Those tickets already
   depend on this ticket. Downstream proof below uses scratch
   checkouts only.

Keep `docs/client-protocol.md` current for the new requests, events,
feature, and conformance revision.

### 7. Live IsolatedHub proof

Extend `tests/hub_daemon_lifecycle/package_event_plane.rs` and add
focused Unix mux and WebRTC tests. Use the existing
`event-plane-producer` package with a `clients` audience contract,
or a sibling synthetic package. Do not name
`botster-workspaces` or Project Pipelines product types in Hub.

Record Hub SHA and locked Core SHA
`8fce2041b9fe742cb2a6df9e74cb262606672742` on the live proof
([[live hub proof records distinct hub and locked core binary provenance]]).

## Non-scope

- Web or TUI event UI (`ticket_1786663584_427840`,
  `ticket_1786663585_944018`).
- Project Pipelines `question.opened` product emit (already closed).
- Saturated-event load campaign (`ticket_1786663585_879846`).
- Replay, public sequence, consumer cursor, durable event flag,
  recovery-family field, wildcard subscription, Hub event history.
- Changing session-family snapshot, gap, or expiry semantics.
- Changing the Unix exclusive entity-subscription socket into a
  multiplexed control socket.
- Terminal fairness, terminal adapter scheduling, Drain, Attach
  translation, or terminal queue policy.
- Floating or bumping the Core pin.
- Web or TUI vendoring of generated protocol (release chain step 4).
- Dual-pipelining teardown-lens implementation.
- New event-plane policy knobs beyond the Stage B table.

## Repository ownership boundaries and cross-repo dependencies

Hub owns host-control admission, client event mailboxes, fair
host-control writing, and the in-repo `botster-hub-client` crate.

`botster-hub-client` owns public DTOs, compatibility descriptors,
generated TypeScript, and conformance revision.

Core owns policy-free `try_admit` and terminal adapters. This ticket
does not change Core.

Terminal bytes stay in SessionIo and ClientWorker. Hub must not
inspect terminal bodies.

Packages own namespaced event names, payload schemas, and product
reactions. Hub contains no Workspaces or Project Pipelines product
names.

Registered prerequisites (all closed and merged into `122d65a`):

| Ticket | Target | Repo | Status |
| --- | --- | --- | --- |
| `ticket_1786663582_483898` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (Stage B router) |
| `ticket_1786661010_198387` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (terminal drain cold-cut) |
| `ticket_1786912569_840742` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (owner-loop background scheduling) |
| `ticket_1786912570_127968` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (deterministic snapshot paging) |
| `ticket_1786912572_610381` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed (PTY process and marker oracles) |

Registered downstream consumers (already present; do not duplicate):

| Ticket | Target | Repo |
| --- | --- | --- |
| `ticket_1786663584_427840` | `tgt_40abcf71ccf049f4ac0c99953a799869` | botster-web |
| `ticket_1786663585_944018` | `tgt_c3d470bab78549df920a41e8fb0e58d8` | botster-tui |

Same-target later sibling, not this scope:

| Ticket | Target | Repo |
| --- | --- | --- |
| `ticket_1786663585_879846` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub Stage D load campaign |

No new Core, Web, TUI, or Workspaces dependency. Do not implement
those repos in this run.

## Assumptions and unknowns

Assumptions:

- Target routing from `list_spawn_targets` is authoritative.
- `botster-hub-client` work belongs in this Hub run because the
  crate lives in this repository.
- Subject filters follow answered
  `question_1786868431_577955`: exact `payload.subject` strings,
  compiled at admission, with the approved ceilings.
- `SubscribeEvents` is not a held-open exclusive stream.
- Connection identity is Hub-assigned (`client_id` or `grant_id`)
  and never appears on public DTOs.
- Event helpers and Hub dispatch require Hello negotiation of
  `package_event_subscriptions` on the host compatibility record,
  not on terminal admission.
- Unix exclusive entity sockets do not carry package events.
  WebRTC carries control, entity, and event frames on one
  DataChannel and must prove fairness among those three.
- `DaemonEvent::WorktreeLifecycle` on mutating responses stays.
- Causal scope stays Hub-internal and is not a public DTO field.
- Existing Stage B policy bounds apply to client egress. No new
  knobs.
- Protocol stays 7. Conformance revision becomes 44.
- Feature `package_event_subscriptions` is advertised and
  operation-specific. Default requirement stays at revision 36.
- Current Core pin `8fce2041` is sufficient.
- The prior-run design decisions survive replay unchanged. The
  charter now canonizes them:
  [[exact owner plus name is the only package event subscription key]],
  [[Package-event subject filters are exact strings compiled at admission]],
  [[Client event holders are connection-scoped]],
  [[Client event subscriptions stay on the multiplexed host-control path]],
  [[Fair host-control writing selects already-admitted frames]],
  [[Host package-event negotiation survives terminal admission rejection]].
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty.
- This is not a session-type eligibility consumer.
- Direct-merge pipeline. No pull request.
- `teardown_class_applies`: no.

Unknowns Implement must resolve by measurement, not invention:

- Exact ready-set order when a WebRTC close-event and a package
  event are both admitted. Both are host Event class. Treat them as
  one Event queue unless a test shows they must be split.
- Whether TUI exhaustive `DaemonRequest` matches break under a
  scratch cargo patch. Record the exact compile cost. Do not change
  TUI here.
- The exact Web install, vendoring or drift, typecheck, and test
  commands in the current `botster-web` checkout. Use that repo's
  real path, not a Cargo patch.
- Whether the registry gains a coordinate above `0.1.38` before
  Implement publishes. If it does, take the next unused version
  above it rather than mutate a published one.
- Which prior test assertions the harness rewrite invalidated.
  Port them to the current guard and oracle idioms by measurement,
  not by weakening the product assertions.

## Affected surfaces and files

- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `src/package_event_router.rs`
- `src/daemon_event_subscriptions.rs` (new)
- `src/host_control_fair_write.rs` (new, small)
- `src/daemon_transport.rs`
- `src/local_webrtc.rs`
- `PendingRuntimeState` host compatibility map, not the terminal
  admission enums
- `src/client_api.rs` or the existing request match in
  `src/daemon_transport.rs`
- `src/lib.rs` module wiring
- `docs/client-protocol.md`
- `packages/hub-test-support/*` version, protocol, matrix, metadata
- `tests/hub_daemon_lifecycle/package_event_plane.rs`
- focused Unix mux and WebRTC tests beside the existing write-path
  tests in `src/daemon_transport.rs` and `src/local_webrtc.rs`
- `examples/event-plane-producer` audience or a sibling clients
  audience contract

Do not edit `src/unix_terminal_adapter.rs` or
`src/webrtc_terminal_adapter.rs` except to prove that this path
makes no adapter API calls.

## Risks

- `SubscribeEvents` copied onto the exclusive Unix entity stream
  would couple events to entity backpressure. Keep it on the
  multiplexed host-control path.
- Adding `DaemonEvent` variants can break old decoders if they are
  sent without negotiation. Emit only to connections that
  negotiated the feature and currently hold the subscription.
- A caller-supplied `subscription_id` without connection scope can
  cross-unsubscribe. Hub must key holders by connection id.
- Gating package events on `UnixTerminalAdmission` or
  `WebrtcTerminalAdmission` drops host negotiation when terminal
  compatibility rejects. Keep a separate host record.
- Fairness that waits for an empty class will delay control
  responses. The scheduler must use only already-admitted frames.
- Host-before-terminal is not terminal fairness. Do not start a new
  terminal slot while a host frame is ready, and do not inspect
  terminal bodies.
- Publishing fixture bytes under `0.1.37` or republishing `0.1.38`
  would violate
  [[Hub test support capability cutovers use a new unpublished package version]].
  The orphan `0.1.38` must stay untouched.
- Closing this ticket without a published support coordinate leaves
  Web and TUI unable to consume a stable artifact
  ([[closed dependency tickets signal merged source not a consumable release]]).
- Hub gravity: keep client egress in a sibling module. Do not grow
  `daemon_transport.rs` with another inline policy well.
- Cherry-pick conflicts in `src/daemon_transport.rs` and
  `src/local_webrtc.rs` are large. A mechanical merge that keeps
  prior-run code but drops a merged-blocker behavior (owner-loop
  scheduling, ShutdownSession suppression, snapshot paging) would
  regress a closed ticket. Re-run the blocker tickets' named tests
  after replay.
- `npm` auth was broken during the prior run (401). Publish may
  need a human, as before.
- A downstream consumer may have installed orphan `0.1.38`. The
  registered Web and TUI tickets pin coordinates explicitly, so the
  supersede path is `0.1.39`; do not deprecate `0.1.38` in this
  run without a human decision.

## Acceptance checks and tests

Wire and unit:

- Serde examples for `SubscribeEvents`, `UnsubscribeEvents`,
  `PackageEvent`, and `EventGap` contain no sequence, cursor,
  replay, or durable-history fields.
- Empty `subjects` omits the field. Generated TypeScript types
  `subjects?`.
- Wildcard, empty, foreign, undeclared, duplicate subject, and
  `plugins`-only audience subscribe requests return typed operator
  errors.
- Boundary tests: 16 versus 17 subjects, 256 versus 257 UTF-8
  bytes, 4,096-byte aggregate, 64 versus 65 subscriptions on one
  connection, duplicate rejection, and multi-byte UTF-8 accounting.
- Compiled subject set matches exact `payload.subject` values and
  drops the rest. Ingress does not inspect the raw request list.
- Same `subscription_id` on two connections stays independent.
  Foreign unsubscribe is a typed miss. Stale cleanup does not
  remove the other connection's holder. A second subscribe on the
  same connection and id is a typed duplicate.
- Router `try_lock` contention on client subscribe or ingress
  returns `shed_busy` and does not block.

Fair write:

- Ready control and event frames: neither class waits for the other.
- Ready control only: write control immediately.
- Ready event only: write event immediately.
- WebRTC: a full event mailbox does not delay a Status or entity
  snapshot that is already admitted.
- Unix mux: a slow event consumer does not delay a Status response
  on the same host-control socket.
- Partial terminal line with nonzero offset still finishes before
  any host frame.
- Unix package-event write resume: a partial `PackageEvent` line
  resumes from its byte offset, does not interleave a Response or
  Terminal line, and leaves the connection open.
- Event flood makes zero terminal adapter API calls and does not
  grow terminal queues.

Live IsolatedHub, Unix and WebRTC:

- Same DTO contract on both transports.
- Producer emit with audience `clients` arrives as an unsolicited
  `PackageEvent` on a negotiated connection.
- Unnegotiated Unix and WebRTC `SubscribeEvents` return a typed
  error. Those connections never receive or decode `PackageEvent`
  or `EventGap`.
- Unix and WebRTC: Hello negotiates `package_event_subscriptions`
  and fails terminal compatibility. Status still succeeds.
  `SubscribeEvents` succeeds. Package events arrive as host
  events. Hub makes no terminal adapter API calls and inspects no
  terminal bodies.
- Fill the mailbox, shed, drain until one slot is free, and emit
  no later event: exactly one `EventGap` arrives. Control and
  entity frames still progress. No replay.
- Disconnect, reconnect, and a fresh `SubscribeEvents` deliver no
  prior event.
- Record Hub SHA, locked Core SHA
  `8fce2041b9fe742cb2a6df9e74cb262606672742`, and binary realpaths
  under this checkout.

Merged-blocker integration:

- After replay, the named tests from the merged blocker tickets
  still pass: owner-loop bounded scheduling, deterministic snapshot
  paging, PTY process and marker oracles, and ShutdownSession
  suppression lanes.
- Package-event egress never enters the owner-loop background
  scheduler, and event pressure never adds owner-loop work.

Compatibility and release:

- Default requirement still accepts the previous descriptor at
  protocol 7 / minimum conformance 36.
- `for_package_event_subscriptions()` rejects that previous
  descriptor and accepts the new one.
- Support matrix lists the new feature only under
  `supported_features`.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  passes.
- TUI downstream: scratch worktree plus Cargo patch onto this
  `botster-hub-client`. Record TUI SHA and compile result. Do not
  commit that worktree.
- Web downstream: scratch `botster-web` checkout against the
  published `@trybotster/hub-test-support` coordinate. Run that
  repo's real install, vendoring or drift, typecheck, and test
  path. Record Hub SHA, package version, and Web SHA. Do not use
  `cargo` on Web. Do not commit that worktree.
- Publish unpublished `@trybotster/hub-test-support@0.1.39` (or the
  next unused version) and prove tokens from a clean install.

Repo gates:

- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `./test.sh --locked` — one clean pass at default concurrency, per
  standing human answer `question_1786875726_281871`. The suite
  blockers are merged, so a flake here is a new finding, not an
  accepted residual.
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo test --doc --workspace`

Direct-merge: Implement commits on this ticket branch. A pull
request is not required.

## Vault gaps worth capturing

The prior run's five design gaps are already captured. The hub
charter now lists them (exact subscription key, compiled subject
filters, connection-scoped holders, multiplexed host-control path,
fair writing, negotiation surviving terminal rejection). Do not
recapture them.

New gaps from this re-entry:

- An unmerged run that publishes an npm coordinate burns it. The
  registry can sit ahead of main (`0.1.38` orphan over in-tree
  `0.1.37`). The next cutover takes the next unused registry
  coordinate and a fresh conformance revision, and never mutates
  the orphan.
- A closed run's implement commits can dangle with no branch.
  Recovery is: find the tip through `git fsck` descendants of a
  known commit, then protect it with a `keep/<ticket>` tag before
  planning a replay.

Capture both after Implement proves the replay, not in Plan.
