# Plan: Cold-cut terminal drains and translation from the production path

Ticket: `ticket_1786661010_198387`
Run: `run_1786754929_522007`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Plan **revision 4** after Plan Review `review_1786756231_948254`, human answer `question_1786756438_318832`, merged Hub fixture-cleanup `ticket_1786664495_777899` at `959c58f55726d098299cced8af151d8f496f41e3`, and merged TUI `ticket_1786756492_156718` at `fc1ff6238ae707c355febbc03eeab5130cccf91c`

## Plan Review corrections (rev 2)

| Finding | Status |
| --- | --- |
| `finding_1786756231_816812` locked Core cannot support no-Drain path | **Locked.** Pin Core `aef6516d5809d563961ed7fdd07da29a7b4edddc` (current `origin/main`, descendant of `f4f6bf5`). Add `HubRuntime` facades for `observe_lifecycle_slice` and `lifecycle_baseline_page`. Replace every production Core terminal Drain caller. |
| `finding_1786756231_365163` HubClientApi Attach unbound | **Locked.** `HubClientApi::Attach` fail-closes and must not call `attach_client`. Successful Attach exists only on Unix/WebRTC transports that bind a real adapter. Negative test: no successful Attach leaves `adapter_bound=false`. |
| `finding_1786756231_320623` terminal authority + protocol | **Prerequisite closed.** TUI `ticket_1786756492_156718` merged at `fc1ff6238ae707c355febbc03eeab5130cccf91c`. Protocol stays 7. This Hub ticket may now remove the three host descriptor tokens. Live Web + TUI (`fc1ff623`) proof against the candidate Hub is still required. |
| `finding_1786756231_879675` incomplete teardown matrix | **Locked.** Matrix now covers Spawn, SubscribeEntities, UnsubscribeEntities, Attach, Detach, Drain, Hello/admission, peer revocation, input/resize, session shutdown, and inventory reconcile, each with tag, reject, sweep, and production-handler proof. |
| `finding_1786756232_279813` sibling fixture ownership | **Closed on main.** `ticket_1786664495_777899` merged at `959c58f55726d098299cced8af151d8f496f41e3` (`origin/main`). That commit is now the Hub base when the TUI dependency closes. Do not reintroduce Hub-owned late-attach GHOSTSNP goldens or `generate_incremental_late_attach_frames.rs`. Late-attach bytes stay Core-owned. |
| `finding_1786756232_517338` disk exhaustion | **Infra.** Recorded. Implement/Verify restore disk before `./test.sh --locked`. Not a product reject. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` (`trybotster/botster-hub`) |
| Plan worktree | this pipeline worktree; Implement rebases onto Hub `959c58f` after the TUI ticket closes |
| Worktree hygiene | tracked `.gitignore` has content (5 lines); path has no `:`; no `CARGO_TARGET_DIR` override required |
| Merge policy | direct into `main`; do not create a PR |

Independent resolution: ticket/run `target_id` plus `list_spawn_targets` map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Routing did not infer the repository from the working directory.

This ticket is **not** a consumer of Hub session-type eligibility work. TUI host-Hello repair `ticket_1786756492_156718` is **closed** and merged at `fc1ff6238ae707c355febbc03eeab5130cccf91c`. All Hub cold-cut dependencies are closed. Implement starts from Hub `959c58f55726d098299cced8af151d8f496f41e3` or newer and proves live TUI at `fc1ff623` plus live Web against the candidate Hub.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] (index only; ownership comes from the Hub charter)
- [[prefer framework and library components over custom solutions]]

In-repo client overlay:

- [[botster-hub-client-playbook]]
- [[botster hub client crate is the external client boundary]]

TUI charter consulted only to route the blocking prerequisite (this run does not implement TUI):

- [[botster-tui-playbook]]
- [[first-party Unix attach clients use split Hello and subscription close events]]

Runtime-teardown class applies:

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths are out of scope
- [[spa-patterns]] — no SPA implementation
- other repository charters as this run's implementation charter

Targeted notes (rev 2 additions marked):

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core owns the incremental attach phase machine]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Core bind stores an immutable negotiated terminal capability set]]
- [[Core ClientWorker bind requires a live attach generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[plan review must check open sibling tickets that own part of the plan scope]]
- [[plan review must verify unmerged unregistered ticket dependencies]]
- [[attach sequence proofs observe the actual Attaching event]]
- [[ready then history capability requires production early drain proof]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[Unix mux host events are unsolicited control frames]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[sessionio hard socket death must fan process exited to clientworkers]]
- [[hub shutdown preserves durable session workers]]
- [[daemon socket attach must detach subscriptions on disconnect and exit]]
- [[pre READY attach failure creates no attach ownership]]
- [[Web paints GHOSTSNP READY while attach remains Attaching]]
- [[vault example paths are not repository placement conventions]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[plan agents must author vault context as wikilinks not home paths]]

## Context loaded

- Plan Review `review_1786756231_948254` verdict `changes_required`
- Human answer `question_1786756438_318832` option 1
- TUI prerequisite `ticket_1786756492_156718` closed; merge `fc1ff6238ae707c355febbc03eeab5130cccf91c` is on `origin/main`
- Closed Web `ticket_1786661008_897067` already requires terminal tokens only on `terminal_compatibility`
- Merged TUI `fc1ff623` requires those three tokens only on `terminal_compatibility`
- Same-target sibling `ticket_1786664495_777899` is **closed**. Merged Hub main is `959c58f55726d098299cced8af151d8f496f41e3`. Goldens/generator are gone; any late-attach fixture consume is Core-owned. Do not restore Hub-owned GHOSTSNP bytes.
- Open same-target integration ticket `ticket_1786661010_115885` is later proof, not fixture ownership
- Production Core terminal Drain callers today: `HubRuntime::drain_runtime_once` / `drain_subscription`; `HubClientApi::DrainRuntime`; `daemon_transport` `DaemonRequest::Drain` and `pump_bound_unix_routes`; `daemon_entity_subscriptions` lifecycle tick; `main.rs` hub smoke Drain-until-attached and `drain_until_marker`; test-only `local_webrtc` peer-loss drain
- Owner loop still calls unbounded `session_lifecycle_baseline()` / `lifecycle_baseline()`. Core `aef6516` supplies `observe_lifecycle_slice` and `lifecycle_baseline_page`
- `HubClientApi::Attach` is unused by daemon Unix/WebRTC Attach; tests and local API still call it and create unbound Core subscriptions
- Published `@trybotster/hub-test-support@0.1.36` / conformance 41 is immutable
- Base fmt/clippy passed; `./test.sh --locked` hit disk exhaustion (116 MiB free)

## Botster layers touched

- Rust Hub daemon Unix/WebRTC transport, owner loop, `HubRuntime`, `HubClientApi`
- In-repo `botster-hub-client` host descriptor, `stream_attach`, Drain helper
- IsolatedHub / lifecycle tests; in-tree hub-test-support without restoring deleted goldens/generator
- Operator smoke / README / `docs/client-protocol.md`
- Not: TUI/Web implementation (TUI is a registered prerequisite ticket); Core implementation beyond the pin; Project Pipelines plugin

## Worktree / target assumptions

- Implement stays in this Hub worktree.
- TUI `ticket_1786756492_156718` is closed. Host-token removal is in scope now.
- Do not edit `botster-tui` or `botster-web` in this run.
- Implement starts from Hub `959c58f55726d098299cced8af151d8f496f41e3` (or a later `origin/main` descendant that still contains that merge).
- Do not reintroduce Hub-owned late-attach GHOSTSNP goldens or `tests/generate_incremental_late_attach_frames.rs`. Late-attach bytes stay on the Core protocol coordinate.
- Merge directly into `main`. Do not create a PR.

## Scope

1. **Pin Core `aef6516d5809d563961ed7fdd07da29a7b4edddc`.** `cargo update` Hub workspace git deps (`botster-core`, `botster-core-daemon`, `botster-terminal-protocol`, `botster-core-test-support`) to that SHA or a later `origin/main` descendant recorded in the Implement report. After the lock update, assert the locked Core SHA.

2. **Replace Core terminal Drain as the production advance path.** Add `HubRuntime` facades:
   - `observe_lifecycle_slice(now, resume, budget)`
   - `lifecycle_baseline_page(...)` with item/byte/elapsed budgets
   Owner loop and entity reconciliation must use those sliced APIs ([[Hub owner loop calls bounded Core lifecycle page APIs]]). Do **not** call compatibility wrappers `observe_lifecycle` or `lifecycle_baseline` on the owner loop.

3. **Delete every Hub production call into Core `drain` / `drain_subscription`.** Exact current callers and replacements:

   | Caller | Today | After |
   | --- | --- | --- |
   | `pump_bound_unix_routes` | `drain_subscription` per bound route | reconcile + close-event queue + `observe_lifecycle_slice` / `pump_bound_adapters` if exposed; no `drain_subscription` |
   | `daemon_entity_subscriptions` lifecycle tick | `drain_runtime_once` | `observe_lifecycle_slice` |
   | `DaemonRequest::Drain` | `drain_subscription` / `DrainRuntime` | host-only response; no Core terminal Drain |
   | `HubClientApi::DrainRuntime` | `drain_runtime_once` | host-only / observe; no Core terminal Drain |
   | `hub smoke` / `drain_until_marker` | Drain-until-attached / `client_egress` TerminalOutput | Attach + bind + `ReadScreen`; no Drain-as-terminal |
   | `local_webrtc` peer-loss test | `drain_subscription` as detach oracle | inventory / adapter-close oracle only |

   After the cut, `rg` over `src/` production items finds no `drain_subscription(` or `drain_runtime_once(` except deleted facades or test-only helpers that do not sit on the daemon owner loop. `lua_runtime` envelope `drain` stays (routed-envelope control, not terminal).

4. **Always bind Unix and WebRTC adapters.** After Hello is not `Rejected` and Core `attach` succeeds with a live inventory generation, bind. No feature-gated unbound residual.

5. **`HubClientApi::Attach` fail-closes.** It must not call `attach_client`. Return a typed operator/client error. Production Attach is only the Unix/WebRTC daemon path that binds an adapter. Negative production test: after any successful Attach, inventory `adapter_bound` is true.

6. **Remove the Attaching exception.** Attach response emits no `AttachState` / Snapshot / TerminalOutput / ProcessExited / Scrollback. Success is a host ack with empty terminal bodies. Failure is `OperatorError`. Discard `attach().client_egress` without translating it. Production must not use leftover readable DTO fields as the terminal path.

7. **Host Drain stays, terminal Drain dies.** `DaemonRequest::Drain` may remain as a protocol-7 readable request. It must not return terminal bodies and must not call Core `drain` / `drain_subscription`.

8. **Delete production terminal polling.** Remove `ATTACH_DRAIN_INTERVAL` and the `stream_attach_connected` sleep/Drain loop. `stream_attach` becomes host completion + `ReadScreen`, or is deleted. No `botster-terminal-protocol-client` dep in Hub or hub-client.

9. **Remove Hub terminal compatibility authority now.** The TUI prerequisite is merged. Remove `FEATURE_TERMINAL_STREAMING`, `FEATURE_RESIZE`, and `FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY` from `current_feature_list()`, `default_required_feature_list()`, and `for_ready_then_history_attach()`. Capability intersection uses `botster-terminal-protocol` / `Hello.terminal_compatibility` only. Do not raise the default host requirement. `PROTOCOL_VERSION` stays 7.

10. **Negative architecture tests.** No READY / PAGE / FINISH branching or GHOSTSNP decode in production Hub sources. Grep gates exclude the one named rejection test.

11. **Compatibility cutover.** Conformance revision 41 → next unused after `npm view` on the `959c58f` tree. In-tree hub-test-support next unused unpublished version. Do not mutate a published version. Do not npm-publish here. Do not restore deleted goldens or `tests/generate_incremental_late_attach_frames.rs`.

12. **Live consumer proof on the candidate Hub.** After the TUI ticket is merged, this ticket's Verify must attach live Web and live TUI to the candidate Hub. Soft residual is not enough.

## Non-scope

- Re-editing TUI Hello (already merged at `fc1ff623`)
- Editing `botster-web`
- Reintroducing Hub-owned late-attach goldens / generator (already deleted by closed `ticket_1786664495_777899`)
- Core attach-and-bind API
- Protocol 8
- Temporary TUI Hello breakage
- npm publish
- Host session shutdown / worktree / retention policy
- Deleting serde variants of unused terminal `DaemonEvent`s (they may remain readable on protocol 7)
- Project Pipelines package work

## Binding decisions

| Topic | Decision |
| --- | --- |
| Core pin | `aef6516d5809d563961ed7fdd07da29a7b4edddc` or recorded later descendant |
| Adapter pump | `observe_lifecycle_slice` / Core pump; never `drain_subscription` |
| Baseline | `lifecycle_baseline_page` with budgets; not `lifecycle_baseline` |
| Unix/WebRTC Attach | Always bind after admitted Hello + live generation |
| `HubClientApi::Attach` | Fail closed; no `attach_client` |
| Attaching handshake | Not a Hub-translated event |
| Protocol | 7. Human option 1. Empty fields readable, unused by production |
| Host terminal tokens | Remove in this ticket. TUI `fc1ff623` already dropped them from host Hello |
| Hub base | `959c58f55726d098299cced8af151d8f496f41e3` or newer `origin/main` |
| TUI live-proof pin | `fc1ff6238ae707c355febbc03eeab5130cccf91c` |
| Late-attach fixtures | Core-owned. Do not restore Hub goldens. |
| Package version | Next unused unpublished after npm + sibling check |

## Runtime-teardown class answers

`teardown_class_applies`: **yes**. Always-bind adapters, remove Hub terminal Drain, and prove one teardown outcome per Detach, connection loss, revocation, ProcessExited, and Hub shutdown.

| Field | Answer |
| --- | --- |
| `teardown_isolation` | One attach owns one adapter, one route, one Core generation. Sibling subscriptions on the same Unix connection or WebRTC peer keep opaque frames. Host workers stay up. ProcessExited closes that session's terminal subscriptions and does not `ShutdownSession`. Spawn creates a host session, not a peer-owned terminal subscription. Entity subscriptions are grant/client-owned and independent of terminal adapters. |
| `teardown_bounds` | Non-blocking `try_write` / `close`. WebRTC local close uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND` then `cleanup_once`. Owner-loop observe/baseline calls use item/byte/elapsed budgets. No `stream_attach` 25 ms Drain loop. Smoke waits are deadline-bounded. |
| `late_message_matrix` | See table. |
| `production_path_proof` | Unix/WebRTC Hello → Attach → inventory generation → bind → owner-loop `observe_lifecycle_slice` pumps adapters. Attach has zero terminal bodies. Drain does not call Core terminal Drain. Live oracles: IsolatedHub Unix+WebRTC; inventory `adapter_bound`; opaque later frames; connection-death adapter close without Hub Detach; explicit Detach; peer loss `cleanup_once`; live Web + live TUI at `fc1ff623` against the candidate Hub. Record Hub SHA and locked Core SHA separately. Terminal JSON / unit helpers are not enough. |
| `ownership_identity` | Hub `(client_id, session_id, subscription_id, generation)` plus Unix `client_id` or WebRTC `grant_id`. Core `(session_id, subscription_id, generation)`. Entity subscription `(client_id or grant_id, subscription_id)`. Spawned session is host-owned by `session_id`. Stale N must not delete N+1. |
| `sibling_fail_closed_policy` | Success: siblings live; host session live. Ultimate adapter close/write-budget: only that subscription dies with the first close reason. ProcessExited does not shut the host session. Entity unsubscribe does not detach terminals. Spawn failure creates no session and no attach. |

### Late-message matrix

| Message | Grant / owner tag | Reject after terminal failure | Residual sweep | Production-handler proof |
| --- | --- | --- | --- | --- |
| WebRTC encrypted Hello / Unix Hello | peer `grant_id` / Unix `client_id` | Dead peer/connection: no new admission | Forget admission row with the owner | Existing Hello reject + PeerClosed/EOF tests stay; add no-regression that late Hello after `remove_peer` creates no grant |
| `Spawn` / `SpawnSessionType` | host session owner after LocalOperator / grant | After peer death, reject or ignore new spawn tied to that grant; Unix spawn is host-scoped | No session row; no attach route | IsolatedHub: spawn then kill peer; spawned session remains listed; no new grant-owned attach |
| `Attach` | Unix `client_id` or WebRTC `grant_id` | `Rejected` admission → `OperatorError` before `start_attach`. Bind/missing-generation → `OperatorError`, no ownership | `cancel_stream` + generation detach | IsolatedHub: successful Attach ⇒ `adapter_bound=true`. `HubClientApi::Attach` errors and leaves inventory empty |
| `bind_terminal_adapter` | live generation + terminal-protocol capability set | `BindBeforeAttach` / stale / already-bound; close rejected adapter | Never enters inventory | Bind-failure IsolatedHub / unit on production helper |
| `Detach` | route owner | Foreign owner forbidden; generation mismatch leaves N+1 | Forward Detach; close leftover adapter | Existing bound Detach proof, keep after always-bind |
| `Drain` | `authorize_drain` if scoped | Foreign owner forbidden | No Core terminal Drain; no Snapshot reconstruction | Drain after bind returns no terminal bodies and does not call `drain_subscription` |
| `SendInput` / `Resize` / `ModeGatedInput` | session owner | After detach, Core rejects; no adapter recreate | None | Existing input/resize tests; no-regression after peer close |
| `SubscribeEntities` | `client_id` / `grant_id` | Post-PeerClosed / dead connection reject | Drop subscription row for that owner only | Production handler: subscribe, close peer, late Subscribe/Unsubscribe neither recreate the peer nor delete a replacement owner's entity sub |
| `UnsubscribeEntities` | subscription owner | Foreign owner forbidden | Remove that entity sub only; terminals untouched | Peer close then stale Unsubscribe is idempotent |
| `ShutdownSession` / `RemoveSession` | host session policy | After session gone, typed cleanup | Suppress terminal close events; adapters closed; workers torn down only by explicit shutdown | Existing shutdown proofs; adapter close ≠ `ShutdownSession` |
| Disconnect / EOF / write failure | Unix `client_id` | No new Attach/bind | Adapter close only; no Hub Detach | Existing Unix revocation proof |
| WebRTC grant revocation / PeerClosed | `grant_id` | No new bind/subscribe for that grant | Close adapters for grant; one `cleanup_once`; full per-peer owner set | Hang-inject close + replacement-owner proof |
| Inventory reconcile | control-plane | Missing Core row → drop Hub route; preserve first close reason | Never infer from silence or empty Drain | Existing reconcile proof; re-run after observe-loop cut |

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This ticket |
| --- | --- | --- |
| Admission, routes, adapters, host Drain, owner-loop observe | Hub | Implement |
| Observe/baseline sliced APIs, bind, inventory | Core `aef6516` | Pin and consume; do not reimplement |
| Terminal tokens | `botster-terminal-protocol` | Intersect only |
| TUI host Hello tokens | Closed `ticket_1786756492_156718` at `fc1ff623` | Live proof only; do not re-edit TUI |
| Web host Hello | Already split | Live proof only |
| Hub GHOSTSNP goldens | Closed `ticket_1786664495_777899` at `959c58f` | Do not reintroduce |
| Integration north-star proof | `ticket_1786661010_115885` | Later; do not absorb |

Registered dependencies:

- Web `ticket_1786661008_897067` — closed
- TUI planes `ticket_1786661009_551067` — closed
- TUI Hello repair `ticket_1786756492_156718` — **closed**, merge `fc1ff6238ae707c355febbc03eeab5130cccf91c`

No new Core ticket. Pin the merged descendant; do not implement Core observe APIs in Hub.

## Assumptions and unknowns

- Human option 1 is the protocol waiver: stay on 7 because live first-party clients are a deployment boundary, not because empty Attach/Drain is “the same contract.”
- TUI `fc1ff623` is merged. This Implement removes the Hub host tokens and proves live TUI + live Web against the candidate Hub.
- Discarding worker `attach().client_egress` Attaching is the cold-cut handshake removal, not a unique Snapshot drop on the worker path.
- `lua.coordination` envelope drain is not a terminal Drain.
- Fixture cleanup is already on `959c58f`. This ticket must not reopen a Hub-owned GHOSTSNP authority.

Unknowns Implement must resolve from the lock, not invent:

- Whether CoreDaemon on `aef6516` exposes `pump_bound_adapters` or only observe-driven pump
- Exact `OperatorError` code for fail-closed `HubClientApi::Attach` and bind failure (reuse an existing attach/admission code)
- Exact unused hub-test-support version/revision after npm + sibling check

## Affected surfaces / files

Create:

- This plan (rev 2) and Implement report under `docs/reports/`
- Negative architecture + fail-closed Attach tests

Edit:

- `Cargo.toml` / `Cargo.lock` — Core pin
- `src/runtime.rs` — observe/baseline facades; stop production Drain facades or leave them unused and `#[cfg(test)]` only if tests still need a name; prefer delete production wrappers
- `src/daemon_attach_stream.rs` — always bind; no attach-egress translation
- `src/daemon_transport.rs` — host-only Attach/Drain; `pump_bound_unix_routes` without `drain_subscription`
- `src/daemon_entity_subscriptions.rs` — `observe_lifecycle_slice` instead of `drain_runtime_once`; sliced baseline instead of `lifecycle_baseline`
- `src/client_api.rs` — fail-closed Attach; DrainRuntime not a terminal Drain
- `src/main.rs` — smoke / `drain_until_marker` → ReadScreen
- `crates/botster-hub-client/src/lib.rs` — delete Drain pump; remove host terminal tokens now
- hub-test-support matrix/metadata/package.json only as required by host-descriptor/conformance cutover; no Hub GHOSTSNP golden restore
- `tests/hub_daemon_lifecycle/*` — always-bind; no unbound Drain oracles
- `README.md`, `docs/client-protocol.md`

Do not edit:

- `crates/botster-hub-test-support/fixtures/ghostsnp/*`
- Deleted goldens / `generate_incremental_late_attach_frames.rs` (do not recreate)
- Do not turn remaining late-attach consume fixtures into Hub-owned GHOSTSNP byte authority

## Risks

- Pinning `aef6516` pulls more than observe/baseline. Stay inside this ticket; do not “fix forward” Core.
- Owner-loop observe still internally drains inside Core. That is Core's control-plane progress, not Hub terminal translation. Hub must not re-expose that egress.
- Removing host tokens before the TUI ticket merges breaks live TUI Hello. Dependency is mandatory.
- Always-bind inserts mux frames on default Unix connections. That is the cold-cut.
- Sibling and this ticket both may need a hub-test-support version. Colliding on 0.1.37 without npm check is a publish-immutability bug.
- Disk was exhausted during Plan Review. Implement/Verify must free space before the suite.

## Acceptance checks / tests

After disk restore:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
BOTSTER_ENV=test cargo clippy --workspace --all-targets --locked -- -D warnings
./test.sh --locked
```

Prove:

1. Locked Core SHA is `aef6516d5809d563961ed7fdd07da29a7b4edddc` or a recorded descendant.
2. `rg` over production `src/` finds no `drain_subscription(` / `drain_runtime_once(` on the owner loop, Drain handler, entity tick, or smoke.
3. Owner loop calls `observe_lifecycle_slice` and `lifecycle_baseline_page` with budgets.
4. IsolatedHub Unix + WebRTC: Attach has no terminal bodies; `adapter_bound=true`; later frames are opaque envelopes only.
5. `HubClientApi::Attach` errors; inventory has no unbound row.
6. Drain request returns no terminal bodies and does not call Core terminal Drain.
7. `ATTACH_DRAIN_INTERVAL` / `stream_attach_connected` sleep loop gone.
8. Smoke is Attach + `ReadScreen`, not Drain-until-attached.
9. Negative READY/PAGE/FINISH / GHOSTSNP guards.
10. Teardown matrix proofs in the table above, driven through production handlers.
11. Host descriptor no longer advertises/requires the three terminal tokens. Live TUI at `fc1ff623` and live Web attach to **this candidate Hub**. Soft residual is not enough.
12. Protocol remains 7. Conformance/package versions are unused and unpublished.
13. Implement starts from `959c58f` (or later main descendant). `git ls-files` still has no Hub-owned late-attach GHOSTSNP goldens and no `generate_incremental_late_attach_frames.rs`.
14. Provenance: Hub SHA, locked Core SHA, binary realpaths.

## Required docs

- `README.md` and `docs/client-protocol.md`
- Implement report
- No Hub GHOSTSNP golden restore
- No npm publish

## Product decision ledger

| Kind | Item |
| --- | --- |
| Default | Pin `aef6516`. Observe/baseline slices. Always bind. Fail-closed local Attach. Protocol 7. |
| Non-goal | Protocol 8, TUI/Web edits, restoring Hub goldens, npm publish |
| Follow-up-ok | Extract leftover readable DTO variants after all consumers ignore them |
| Ask-human | Answered: stay protocol 7; TUI first; no Hello break |

## Pipeline gates and artifacts

- Plan artifact: this file, rev 2.
- Vault checklist: reuse `checklist_1786755299_973250` (skip duplicate; same ticket Plan visit already has one).
- Gate evidence must include `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, `target_repository`.

## Vault gaps worth capturing

- After TUI + Hub ship: host Hello no longer carries Core terminal mechanism tokens; protocol 7 stayed because of a live-client deployment boundary, not because Drain remains a terminal path.
- After Implement proof: Hub production advances adapters through observe slices, not `drain_subscription`.
- Do not ratify the full north star from this ticket alone.
