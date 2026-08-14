# Plan: Split Hello terminal compatibility and emit TerminalSubscriptionClosed

Ticket: `ticket_1786705502_228757`
Run: `run_1786705508_262530`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Required by TUI ticket `ticket_1786661009_551067`
Human answer `question_1786705427_821834`: **1B**
Plan **revision 2** after Plan Review `review_1786706783_709010`

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786706783_225912` preserve failed terminal admission through Attach | product / high | Register one per-connection `UnixTerminalAdmission` on every Unix Hello. The value is `Admitted` or `Rejected`. Attach reads that result **before** `start_attach` / `begin_core_attach`. Missing admission is only the non-Unix path. |
| `finding_1786706783_194723` invalid Web Cargo downstream check | product / high | Keep scratch Cargo patch for TUI only. Web proof uses `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` against this worktree's generated `daemon-protocol.ts`, then `npm test`. |
| `finding_1786706783_161403` process-exit event semantics | product / medium | Remove `process_exited`. Do not emit `TerminalSubscriptionClosed` when Hub session lifecycle already shows process exit. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` |
| Plan worktree | this pipeline worktree; Plan does not mutate `Cargo.lock` |
| Worktree hygiene | tracked `.gitignore` has 53 bytes matching HEAD; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `9d1f858fbfaf87ff2e95cf292690b03e91558695` |
| Locked Core | `Cargo.lock` pins `botster-core` `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** |
| `teardown_class_applies` | **yes** |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Routing did not use the process working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — planner Must Load only. This ticket has no React/SPA edit surface.
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[prefer framework and library components over custom solutions]]
- [[botster pipeline needs continuous product owner between agent steps]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[cross repo dependency registration must use dependency repo target]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Repository overlay for public DTO work inside this repo:

- [[botster-hub-client-playbook]]

Runtime-teardown class applies. Loaded:

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- other repository charters — this run stays on `botster-hub`

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub client compatibility descriptors belong in client crate]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[published capability matrices must derive enumerations from source]]
- [[ready then history is advertised as optional daemon support]]
- [[botster terminal v1 starts at protocol 1 and conformance revision 1]]
- [[Core bind stores an immutable negotiated terminal capability set]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Core ClientWorker bind requires a live attach generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[proposed Hub admission binds adapters with negotiated subscription capabilities]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[Hub route registry names describe ownership not attach queues]]
- [[session shutdown during attach does not produce attach failed]]
- [[pre READY attach failure creates no attach ownership]]
- [[attach failed cleanup is route aware and idempotent]]
- [[test script required for rust tests not cargo test]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[adding a hub client feature constant is a three site change]]
- [[botster web generated protocol drift checks need explicit hub artifact paths]]

## Context loaded

Project: Botster Terminal Transport North Star. Hub is the public endpoint and host policy authority. Core owns terminal subscriptions, attach phases, queues, slow-client policy, and adapter hard-stop. Hub must not inspect terminal bodies.

This ticket exists because TUI Plan Review found two Hub gaps on `9d1f858`:

1. `DaemonHelloAck` advertises only host `DaemonCompatibility`. There is no live terminal compatibility exchange.
2. Adapter close and Core write-budget hard-stop leave no host-control signal while the socket stays up. TUI cannot bound lost-PAGE or slow-client recovery.

Human 1B locked the Hub contract:

- Send Core `TerminalCompatibilityRequirement` on Hello.
- Receive independent Core `TerminalCompatibility` on `DaemonHelloAck`.
- Keep host `DaemonCompatibility` separate.
- Hub admits, intersects, and binds the immutable negotiated set on each Core subscription.
- A terminal mismatch is a typed admission error before Attach.
- Emit `TerminalSubscriptionClosed` when a bound subscription closes and the connection stays alive.
- Do not reuse `AttachFailed`.
- Carry subscription identity, generation, and a stable close reason when Core provides one.
- Observe adapter and route lifecycle only.

Current Hub code on `9d1f858`:

- `DaemonHello` / `DaemonHelloAck` carry only host compatibility. Hub writes the ack and ignores the host requirement. The client checks host compatibility.
- `RegisterUnixAdmission` stores host `required_features` and the connection mux.
- `bind_unix_adapter_after_attaching` computes `TerminalCapabilitySet` from host features plus `TerminalCompatibility::current()`. Bind happens after Core attach creates a generation.
- Pre-bind failure still emits `AttachFailed`.
- `UnixConnectionMux` notify fires on write and close. `flush_unix_adapter_envelopes` skips closed handles and writes nothing.
- Core inventory (`TerminalSubscriptionRecord`) has identity, generation, `adapter_bound`, and `capabilities`. It has no close reason.
- Core write-budget hard-stop removes the owner, then calls non-blocking `adapter.close()` and drops the adapter on the same tick.
- `botster-hub-client` has no `botster-terminal-protocol` dependency. Host protocol is version 7, conformance 39. `@trybotster/hub-test-support` is `0.1.34`.

Closed parents that this plan consumes:

- Core types-only terminal protocol
- Core adapter contract and conformance harness
- Core ClientWorker egress and hard-stop
- Core immutable capability bind (`f4f6bf5`)
- Hub Unix content-blind adapters (`9d1f858`)

Open siblings that this run must not implement:

- Hub cold-cut of terminal drains (`ticket_1786661010_198387`)
- TUI consumer (`ticket_1786661009_551067`) — already depends on this ticket
- Web consumer (`ticket_1786661008_897067`)
- Hub WebRTC adapter ticket
- Integration north-star proof

## Product decision ledger

| Kind | Decision |
| --- | --- |
| Default | Always advertise Core `TerminalCompatibility::current()` on HelloAck. |
| Default | Host `DaemonCompatibility` stays a separate field and a separate check. |
| Default | Negotiate one immutable `TerminalCapabilitySet` at Hello. Bind that same set on every later Core subscription for the connection. |
| Default | Keep `PROTOCOL_VERSION` at 7. Bump `CONFORMANCE_FIXTURE_REVISION` 39 → 40. |
| Default | Keep `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` at 36. Do not raise the default host requirement. |
| Default | A missing terminal requirement is not a mismatch. Status-only clients keep working. |
| Default | A present terminal requirement that fails `ensure_compatible` stores `UnixTerminalAdmission::Rejected` on that connection. HelloAck still returns. The connection stays up for host operations. Attach reads that result before `start_attach` and returns `OperatorError`. |
| Default | Absence of `unix_admissions[client_id]` means a non-Unix path. It is not a terminal rejection. |
| Default | `TerminalSubscriptionClosed` is an unsolicited mux-classified host event. It is not a request reply and not `AttachFailed`. |
| Default | Emit only for a bound route that closes while the same connection stays alive. |
| Default | Do not emit on connection death, `mux.close_all()`, or client-initiated Detach. |
| Default | Close reasons are only `host_adapter_closed` and `core_adapter_closed`. There is no `process_exited` reason. |
| Default | If Hub session lifecycle already shows process exit or session removal, do not emit `TerminalSubscriptionClosed`. Process exit stays on lifecycle / `ProcessExit` paths. |
| Non-goal | Inspect READY, PAGE, FINISH, Snapshot, or other terminal bodies. |
| Non-goal | Build a second attach phase machine. |
| Non-goal | Cold-cut remaining Hub terminal Drain translation. |
| Non-goal | Implement TUI, Web, or WebRTC adapter consumers. |
| Non-goal | Add a Core close-reason API in this run. |
| Non-goal | Create a pull request. |
| Follow-up ok | npm publish of a new hub-test-support version after in-repo fixture bump. |
| Follow-up ok | WebRTC emission after that adapter ticket binds Core adapters. |
| Ask human if | Implement cannot classify a close without decoding a terminal body. |
| Ask human if | A live production path cannot emit the event without stealing a request reply. |
| Ask human if | A required protocol-8 flag day appears after downstream compile measurement. |

## Scope

1. Add a Core-owned `TerminalCompatibilityRequirement` field to `DaemonHello`.
2. Add an independent Core `TerminalCompatibility` field to `DaemonHelloAck`. Keep host `DaemonCompatibility`.
3. At Hello, admit the terminal requirement, compute the capability intersection, and store one immutable `TerminalCapabilitySet` on the Unix admission.
4. Bind that stored set on each later Core subscription. Do not recompute a different set at Attach.
5. Fail a terminal-plane mismatch with a typed admission error before Attach. Do not create attach ownership.
6. Emit `TerminalSubscriptionClosed` when a bound adapter/route closes and the connection remains alive.
7. Include `session_id`, `subscription_id`, generation, and a stable Hub-observed reason.
8. Keep the Unix adapter content-blind. Keep the Core adapter harness green.

## Non-scope

- Hub cold-cut of production terminal Drain / translation
- WebRTC DataChannel adapters
- TUI or Web client consumption
- Core protocol, inventory, or close-reason changes
- Raising the default host or terminal requirement
- Dual production paths or feature flags for the new Hello fields
- Session-type eligibility work

## Repository ownership boundaries and cross-repo dependencies

Hub owns:

- Hello admission
- Host `DaemonCompatibility`
- Capability intersection with host grants
- Adapter instances, mux, framing, and route ownership records
- Host-control event `TerminalSubscriptionClosed`

Core already owns, and this run must consume without editing Core:

- `TerminalCompatibility` / `TerminalCompatibilityRequirement` / `ensure_compatible`
- `TerminalCapabilitySet`
- Subscription identity `(session_id, subscription_id, generation)`
- Bind, inventory, write-budget hard-stop, and adapter `close()`

`botster-hub-client` lives in this repository. Public DTO work stays here. Load [[botster-hub-client-playbook]] as an overlay, not as a second target.

Do not register a Core dependency. Locked Core `f4f6bf5` already ships the types and bind API. Core inventory has no close reason. The ticket allows a reason "when Core provides one." Hub-observed reasons satisfy the ticket.

Do not spawn TUI or Web work in this run. TUI ticket `ticket_1786661009_551067` already depends on this Hub ticket and target `tgt_c3d470bab78549df920a41e8fb0e58d8`. Web remains a later consumer on `tgt_40abcf71ccf049f4ac0c99953a799869`.

## Assumptions and unknowns

Assumptions:

- Protocol 7 plus conformance 40 is enough. Old clients that never attach do not receive `TerminalSubscriptionClosed`. First-party attach clients update through their own tickets after this merge.
- Hello mismatch keeps the socket open for Status and other host operations. It only blocks terminal admission and Attach.
- `botster-hub-client` may depend on types-only `botster-terminal-protocol`. It must not depend on `botster-core` runtime or `botster-terminal-protocol-client`.
- Optional host feature `terminal_subscription_closed` is advertised and not required.
- Remaining Hub Drain translation stays until the cold-cut ticket. This ticket does not add a second phase machine.
- Web has no Cargo crate. Downstream Web proof is the generated TypeScript artifact, not a Cargo patch.

Unknowns that Implement must prove, not invent:

- Exact `DaemonUnixMuxFrame` classification that cannot be mistaken for a request reply
- Exact write-budget stall that trips Core hard-stop on the production Unix flush path
- Exact TUI Rust literal cost after the Hello field additions

## Affected surfaces/files

Public contract:

- `crates/botster-hub-client/Cargo.toml` — add types-only `botster-terminal-protocol`
- `crates/botster-hub-client/src/lib.rs` — Hello fields, HelloAck field, `DaemonEvent::TerminalSubscriptionClosed`, optional host feature, conformance 40, mux parse, connect helper
- `crates/botster-hub-client/src/typescript.rs` and `generated/daemon-protocol.ts`
- `docs/client-protocol.md`
- `packages/hub-test-support/` — regenerate protocol artifact and support matrix; bump unpublished package version if published `0.1.34` bytes would change

Admission and bind:

- `src/daemon_transport.rs` — Hello ack, `RegisterUnixAdmission` with `UnixTerminalAdmission::{Admitted,Rejected}`, mismatch diagnostic, Attach gate **before** `start_attach`, unsolicited close emit
- `src/daemon_attach_stream.rs` — bind only the stored admitted set; do not treat close as `AttachFailed`
- `src/unix_terminal_adapter.rs` — host-close vs Core-close flag on the shared handle; no body inspection
- `src/runtime.rs` / `src/client_api.rs` only if they construct Hello or events
- `src/daemon_projection.rs` if `DaemonStatus.compatibility` needs the new optional host feature

Tests and docs:

- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `src/daemon_transport.rs` and `crates/botster-hub-client/src/lib.rs` unit tests
- `docs/plans/split-hello-terminal-compatibility-and-emit-terminal-subscription-closed.md`
- Implement report under `docs/reports/`

## Implementation plan

### 1. Public Hello split

Add to `DaemonHello`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub terminal_compatibility: Option<TerminalCompatibilityRequirement>,
```

Add to `DaemonHelloAck`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub terminal_compatibility: Option<TerminalCompatibility>,
```

Production HelloAck always sets `Some(TerminalCompatibility::current())`.

Keep `compatibility: DaemonCompatibility` unchanged.

Re-export the Core types from `botster-hub-client`. Do not copy the structs.

Extend `connect_and_hello_with_requirement` with a sibling that can send a terminal requirement. Do not force the terminal requirement into the default host helper.

Bump `CONFORMANCE_FIXTURE_REVISION` to 40. Advertise optional host feature `terminal_subscription_closed`. Do not add it to `DaemonCompatibilityRequirement::current()`.

### 2. Hello admission and intersection

Replace today's `UnixAdmission { required_features, mux }` with one durable per-connection result keyed by `client_id`:

```rust
enum UnixTerminalAdmission {
    Admitted {
        required_features: Vec<String>,
        capabilities: TerminalCapabilitySet,
        mux: UnixConnectionMux,
    },
    Rejected {
        code: &'static str, // "terminal_compatibility"
        diagnostic: DaemonDiagnostic,
    },
}
```

Every Unix Hello registers exactly one of those variants. Absence of the map entry means a non-Unix connection. Absence is not a rejection.

After a valid host Hello:

1. Write HelloAck with both descriptors.
2. If `hello.terminal_compatibility` is `Some(req)` and `ensure_compatible(req, &TerminalCompatibility::current())` fails:
   - Add a typed diagnostic on HelloAck.
   - Register `UnixTerminalAdmission::Rejected { code: "terminal_compatibility", diagnostic }`.
   - Leave the connection open for host operations. Do not create a mux bind path.
3. If compatible or omitted:
   - Compute one `TerminalCapabilitySet`:
     - Start from `TerminalCompatibility::current().features`.
     - Keep tokens the host grants.
     - Include `snapshot_delivery=ready_then_history` only when the client terminal requirement or the host Hello feature list asks for it.
   - Register `UnixTerminalAdmission::Admitted { required_features, capabilities, mux }`.

On Attach, in `handle_control_request` for `DaemonRequest::Attach`, **before** `start_attach` and **before** `begin_core_attach`:

1. If `unix_admissions.get(client_id)` is `Some(Rejected { .. })`, return `DaemonResponseKind::OperatorError` with `DaemonOperatorError { code: "terminal_compatibility", operation: "attach", ... }`. Do not call `start_attach`. Do not call `begin_core_attach`. Do not emit `AttachFailed`. Create no Hub route and no Core inventory row.
2. If `Some(Admitted { capabilities, mux, .. })` and the connection requires the Unix adapter, continue the current attach-then-bind path. Bind the stored `capabilities`. Do not recompute a different set.
3. If `None`, keep today's non-Unix attach path.

Production-path test: send a mismatched `TerminalCompatibilityRequirement` on Hello, then send Attach on that same connection. Prove `OperatorError`, empty Core `list_terminal_subscriptions` for that identity, and no adapter bind.

This ratifies the Hub half of [[proposed Hub admission binds adapters with negotiated subscription capabilities]] for Unix Hello. It does not ratify WebRTC.

### 3. TerminalSubscriptionClosed

Add:

```rust
TerminalSubscriptionClosed {
    session_id: String,
    subscription_id: String,
    generation: u64,
    reason: String,
}
```

Deliver it as a mux-classified host event. Extend `parse_unix_mux_value` / `DaemonUnixMuxFrame` so the frame is not a `DaemonResponse` request reply.

Emit from the production Unix connection loop when `mux.notify()` fires and a bound handle is newly closed while the writer is still alive.

Stable reasons:

| Reason | Observation |
| --- | --- |
| `host_adapter_closed` | Hub closed this handle and the connection stays up |
| `core_adapter_closed` | Handle closed without the host-close flag. This covers Core write-budget hard-stop and other Core `adapter.close()` paths |

There is no `process_exited` reason on this ticket.

Do not emit when:

- the connection is dying (`close_all`, EOF, shutdown, write failure)
- the client just received a Detach response for that generation
- the generation is not the bound generation
- Hub session lifecycle already shows process exit or session removal. That case stays on lifecycle / `ProcessExit`. Emitting `TerminalSubscriptionClosed` would make TUI start a fresh attach.

Mark each route reported once. A stale generation N event must not close generation N+1. Replacement-owner proof needs a Hub route-ownership oracle, not mux byte delivery.

`TerminalSubscriptionClosed` is not `AttachFailed`. Pre-bind failure may still use `AttachFailed`. Session shutdown during attach stays on the lifecycle/process-exit path.

### 4. Keep the adapter content-blind

`UnixTerminalAdapter::try_write` continues to serialize opaque `TerminalFrame` bytes only. Close observation uses `is_closed()`, the host-close flag, and route identity. Hub session lifecycle is used only as a suppress rule for already-exited sessions. No READY, PAGE, FINISH, or Snapshot decode.

## Runtime-teardown lens answers

| Field | Answer |
| --- | --- |
| `teardown_class_applies` | yes. Bound subscription close while the connection stays alive is terminal-state vs live-runtime divergence. Write-budget hard-stop is ClientWorker teardown. |
| `teardown_isolation` | One subscription owner dies: `(client_id, session_id, subscription_id, generation)` plus its adapter handle and Hub route record. Sibling routes on the same mux stay up. Host session stays up. |
| `teardown_bounds` | Adapter `close()` is non-blocking. Emit is a single mux write on the existing notify path. No `block_on(close)` on the control thread. A hung library close is an adapter defect, not a new Hub closer thread. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | Live Unix path: adapter close or Core hard-stop → handle `closed` → mux notify → connection loop → one `TerminalSubscriptionClosed` → connection idle and still accepted for host requests. Prove with isolated Hub binary plus locked Core worker. A JSON fixture alone is not proof. |
| `ownership_identity` | `(client_id, session_id, subscription_id, generation)`. Reused `subscription_id` increments generation. Event for N must not sweep N+1. |
| `sibling_fail_closed_policy` | Success: siblings keep working. Ultimate emit/write failure: fail only that route's reporting; do not close sibling adapters. Connection write failure is existing connection death and must not emit this event. Test sibling survival for both close causes. |

Late-message matrix:

| Message | Tag | After close | Sweep |
| --- | --- | --- | --- |
| Hello | connection admission | already complete | none |
| Attach | client_id + session + subscription | If `unix_admissions` is `Rejected`, `OperatorError` before `start_attach`. If `Admitted`, new attach after close creates generation N+1 and a new bind. If missing, non-Unix path | do not attach a rejected or closed generation |
| Detach | session + subscription + live generation | `AlreadyGone` or generation mismatch | no `TerminalSubscriptionClosed` |
| SendInput / Resize | same owner key | typed reject / no-op after Core owner is gone | no new owner |
| Drain | route owner | existing route-aware fail-closed | do not decrement another route |
| SubscribeEntities | grant/client | unchanged | unchanged |
| `TerminalSubscriptionClosed` for N after N+1 is live | generation | ignore / do not emit for N+1 | must not close N+1 |
| Connection EOF / PeerClosed | client_id | connection cleanup, `close_all` | no `TerminalSubscriptionClosed` |
| ShutdownSession / process exit | host session id | lifecycle / `ProcessExit` only | no `TerminalSubscriptionClosed` |

## Risks

- Unsolicited `DaemonResponse` would steal a request reply. Mux classification is mandatory.
- Adding Hello fields is wire-additive and Rust-source-breaking. Measure TUI with a scratch Cargo patch. Measure Web with the generated TypeScript drift check, not Cargo.
- A new `DaemonEvent` variant is source-breaking for exhaustive matches. Keep protocol 7 only if old non-attach clients never see the event.
- Emitting `TerminalSubscriptionClosed` after process exit would make TUI start a fresh attach. Suppress the event when Hub session lifecycle already shows exit. Do not add a `process_exited` reason.
- Mux envelope delivery is not a Hub ownership oracle. Replacement tests must assert route identity and generation.
- Remaining Drain translation can still emit `AttachFailed`. Do not reuse that path for post-bind close.
- Fixture edits under published `@trybotster/hub-test-support@0.1.34` are forbidden. Bump to a new unpublished version if those bytes change.
- Live hub target dirs can cache stale same-version `botster-hub-client` artifacts. Refresh `BOTSTER_LIVE_HUB_TARGET_DIR` for live proof.

## Acceptance checks/tests

In-repo proofs:

1. HelloAck has `terminal_compatibility` independent from host `compatibility`.
2. Host default requirement still accepts the previous host descriptor. Optional `terminal_subscription_closed` is supported, not required.
3. A terminal-plane mismatch writes a typed diagnostic on Hello, stores `UnixTerminalAdmission::Rejected`, then returns `OperatorError` on the next Attach **before** `start_attach`. No `AttachFailed`. No Core inventory row. No bind. The connection still answers Status.
4. A compatible Hello stores `UnixTerminalAdmission::Admitted` with one set. Inventory after bind echoes those tokens.
5. Host adapter close on one bound route emits one `TerminalSubscriptionClosed` with identity, generation, and `host_adapter_closed`. Connection stays up. Sibling route survives. No READY/PAGE/FINISH decode.
6. Authentic Core write-budget hard-stop emits one `TerminalSubscriptionClosed` with `core_adapter_closed`. Sibling survives. No body decode.
7. Connection death and explicit Detach do not emit the event.
8. Session process exit / `ShutdownSession` while the Unix connection stays up does not emit `TerminalSubscriptionClosed`. Existing lifecycle / `ProcessExit` events may still appear. No `process_exited` reason exists.
9. Stale close for generation N leaves generation N+1 owned. Use a Hub route oracle, not mux bytes.
10. Unix adapter still passes the Core conformance harness.
11. Generated TypeScript marks new skippable fields optional.
12. Support matrix derives features and conformance 40 from source.

Commands:

```sh
./test.sh --test hub_daemon_lifecycle_test unix_adapter
./test.sh -p botster-hub-client
./test.sh -p botster-hub-test-support
./test.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Use `./test.sh`, not bare `cargo test`. If a filter matches zero tests, treat that as a failed proof.

Downstream proof required by the hub-client overlay:

- TUI `tgt_c3d470bab78549df920a41e8fb0e58d8`: scratch Cargo patch against this `botster-hub-client`. Record compile failures. Do not edit TUI in this run.
- Web `tgt_40abcf71ccf049f4ac0c99953a799869`: no Cargo crate. From a scratch Web worktree run:

  ```sh
  BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL=<this-hub-worktree>/crates/botster-hub-client/generated/daemon-protocol.ts \
    npm test
  ```

  That command runs `scripts/check-daemon-protocol-drift.mjs` plus `src/App.test.mjs`. A skipped drift check is not evidence. Record the exact artifact path and whether the vendored Web file differs. Do not edit Web in this run.
- If Hub test-support package bytes change, bump the in-repo unpublished coordinate from `0.1.34` to `0.1.35` at `packages/hub-test-support` and sync `packages/hub-test-support/daemon-protocol.ts` from the generated hub-client artifact. Do not publish. Do not mutate published `0.1.34` registry bytes. Web's check in this ticket consumes the generated hub-client file, not a published npm version.
- Live isolated Hub proof records this Hub SHA and locked Core `f4f6bf5`. Resolve both binary realpaths.

TUI live attach/hydration proof belongs to `ticket_1786661009_551067` after this ticket closes. This run must leave a consumed artifact: Hello fields, advertised `terminal_compatibility`, and `TerminalSubscriptionClosed` on the production Unix path.

## Vault gaps worth capturing

- Capture after Implement if mux-classified host events become a durable convention.
- Capture after Implement if Hello keeps host operations open after a terminal mismatch, including the durable `UnixTerminalAdmission::Rejected` row.
- Do not capture a Core close-reason API unless a later ticket needs more than Hub-observed reasons.

No capture this Plan visit. The required conventions already exist.
