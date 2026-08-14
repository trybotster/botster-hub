# Plan: Emit core_adapter_closed while Unix host mux stays readable

Ticket: `ticket_1786716545_417854`
Run: `run_1786717046_410510`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Parent / consumer: TUI ticket `ticket_1786661009_551067`
Review finding: `finding_1786715974_781287`
Sibling Hub ticket (not this run): `ticket_1786716545_950076` (hub-client terminal-protocol pin)
Plan **revision 3** after Plan Review `review_1786718485_761426`

## Plan Review corrections (rev 2 → rev 3)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786718486_988285` Ordered responses must preserve delivery acknowledgement and close sequencing | product / high | Each queued Response carries its optional `response_delivery_tx` and `close_after_response` flag on `PendingMuxFrame`. Fire the ack only after that JSON line is fully written. Do not return from `handle_connection_async` or `close_all` while a close-after Response is pending. Bound the wait with existing `DAEMON_CLIENT_WRITE_TIMEOUT` (2s) plus 50ms write slices. Add focused tests: partial terminal frame, then `StartHubUpdate` / `DaemonShutdown`, then two complete mux lines and ack-after-write. |

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786718123_945752` Direct responses can interleave with a partial terminal mux frame | product / high | Serialize Response, Event, and Terminal through one `MuxWriteState`. `write_async_frame` must not write a Response while a terminal JSON line has a nonzero offset. Finish that partial line, or abandon a zero-offset terminal start, then write the Response through the same ordered writer. Keep the 50ms bound. Add a unit test that starts a partial terminal line, introduces a Response, resumes, and parses two complete ordered mux lines. |
| `finding_1786718123_532087` The listed integration command filters out the ticket proof | product / high | Drop the `unix_adapter` cargo filter. List exact `-- --exact` commands for every required close test. Record that `./test.sh --offline --test hub_daemon_lifecycle_test unix_adapter` ran 8 / filtered 178 and does not execute `core_write_budget_hard_stop_emits_core_adapter_closed`. |
| `finding_1786718123_514376` Status only after read timeout can miss the open-adapter window | product / medium | Require one successful Status round trip after pressure starts and **before** the `core_adapter_closed` Event. Schedule by elapsed time or collected flood-frame count, not only read timeout. Keep reading. Assert Response ordering on the mux. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` |
| Plan worktree | this pipeline worktree; Plan does not mutate `Cargo.lock` |
| Worktree hygiene | tracked `.gitignore` is 53 bytes matching HEAD; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `aafd6c2cde430804f1bb54094c568fc88c15944b` |
| Locked Core | `Cargo.lock` pins `botster-core` `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** (this is a Hub producer ticket, not a Hub session-type eligibility consumer) |
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

Repository overlay for public DTO consumption inside this repo:

- [[botster-hub-client-playbook]]

Runtime-teardown class applies. Loaded:

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- other repository charters — this run stays on `botster-hub`
- [[botster-tui-playbook]] — ticket forbids editing `botster-tui`; TUI consumption is a downstream consumer of this artifact

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[Unix mux host events are unsolicited control frames]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[graceful-termination-requires-explicit-cleanup-hooks]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]

## Context loaded

- Ticket intent: TUI live proof can stall the whole Unix mux and then observe `TerminalSubscriptionClosed` with reason `host_adapter_closed`. That proves host socket egress pressure. It does not prove Core write-budget hard-stop (`512`-tick `core_adapter_closed`) while host frames continue.
- Parent finding `finding_1786715974_781287`: stalling the Unix mux is not the Core oracle; require exact `core_adapter_closed`; keep a live sibling producing frames; if Hub cannot expose that proof, register a Hub/Core dependency instead of widening the TUI oracle.
- Shipped Hub `aafd6c2` already defines `TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER` / `HOST_ADAPTER`, emits `DaemonEvent::TerminalSubscriptionClosed` from `UnixConnectionMux::queue_closed_subscription_events`, and has isolated-hub test `core_write_budget_hard_stop_emits_core_adapter_closed`.
- That existing test is insufficient for this ticket:
  - Trigger is a `yes` flood on the shared socket.
  - `flush_unix_mux_writes` writes every terminal slot **before** host Events.
  - One `MuxWriteState` pending frame can occupy the socket; a write error calls `mux.close_all()`, which `close_from_host()`s every route.
  - Sibling proof is `ListSessions` lifecycle `running`, not live terminal frames.
  - A consumer that stops or slows mux reads therefore observes `host_adapter_closed`.
- Core `f4f6bf5` already owns the 512-tick close. `accepted_in_flight_write_counts_toward_the_write_budget` proves: hold one adapter's accepted write, sibling auto-completes, only the stalled subscription dies, sibling still delivers. No Core ticket.
- IsolatedHub launches `CARGO_BIN_EXE_botster-hub`. There is no in-process test hook into the live mux. The proof must use the production Unix path.

## Scope and non-scope

### Scope

- Keep exposing authentic Core adapter close with reason exactly `core_adapter_closed` on the Unix mux Event plane.
- Route Response, Event, and Terminal frames through one ordered `MuxWriteState` so a Status Response cannot split a partial terminal JSON line.
- Carry `response_delivery_tx` and `close_after_response` on the queued Response. Ack after the line is written. Do not close while that Response is pending.
- Change the Unix connection flush so host control frames (`Response` and `Event`) stay writable while one Core adapter hard-stops.
- Isolate one subscription's occupied adapter slot from filling or killing the shared mux.
- Keep a sibling subscription on the **same** Unix connection live and producing terminal frames.
- Keep the client reading the mux for the whole proof. Do not create the Core close by stopping reads.
- Reject `host_adapter_closed` as the Core oracle.
- Add IsolatedHub + `botster-hub-client` proof that TUI can consume without editing TUI.
- Update `docs/client-protocol.md` only as needed to state the host-readable / sibling-live contract.

### Non-scope

- Do not edit `botster-tui`.
- Do not register a Core ticket. Core already emits the 512-tick close on the adapter.
- Do not treat sibling ticket `ticket_1786716545_950076` as this run. Protocol-identity pin is a separate Hub target ticket.
- Do not add a daemon request, feature flag, or knob to hold a route.
- Do not synthesize `core_adapter_closed` without Core calling adapter `close()` (no `host_closed` flag).
- Do not change Hello, terminal compatibility, Detach, process-exit, or `ShutdownSession` suppression rules.
- Do not inspect READY, PAGE, FINISH, or Snapshot bodies to decide the close reason.
- Do not change WebRTC adapters.
- Do not bump `PROTOCOL_VERSION` or raise the default client requirement. Conformance 40 already advertises `terminal_subscription_closed`.
- Do not publish hub-test-support or change UI-contract tags.
- Do not create a PR.

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This run |
| --- | --- | --- |
| Unix mux flush, route records, adapter handles, host Event emit | Hub | **edit** |
| `botster-hub-client` Event DTO and `core_adapter_closed` constant | Hub client crate in this repo | consume; edit only if a tiny consumer helper is required |
| 512-tick write-budget and adapter `close()` | Core `f4f6bf5` | **already shipped**; no Core ticket |
| TUI live attach / recover | TUI `tgt_c3d470bab78549df920a41e8fb0e58d8` / `ticket_1786661009_551067` | consumer; already depends on this ticket; do not edit |
| Terminal-protocol crate identity | Hub ticket `ticket_1786716545_950076` | sibling; do not broaden this run |

No new cross-repo dependency to register. A Core ticket would use `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`, not this Hub target. That is not required.

## Product decision ledger

| Kind | Decision |
| --- | --- |
| Default | Same-connection sibling is required. Two connections are not the proof. |
| Default | The client keeps reading. Status round-trips during the wait are the keep-reading oracle. |
| Default | Close reason must be exactly `core_adapter_closed`. `host_adapter_closed` fails the proof. |
| Default | Core close is adapter `close()` with `host_closed == false` after 512 unsuccessful or in-flight ticks. Hub must not `close_from_host()` that route to create the event. |
| Default | One ordered mux write state owns Response, Event, and Terminal JSON lines. `handle_connection_async` must not call `write_async_frame` for a Response while `mux_write.pending` holds a partial terminal line. |
| Default | `StartHubUpdate` and `DaemonShutdown` Responses carry the existing `response_delivery_tx` on the pending frame. Ack fires only after that Response line is fully written. `close_after_response` waits through 50ms Pending slices until Written or `DAEMON_CLIENT_WRITE_TIMEOUT`. Do not close the connection while that Response is pending. |
| Default | Host Events and Responses flush before new terminal slots. A stalled terminal JSON line that wrote 0 bytes is abandoned; the adapter slot stays occupied. A partial JSON line (offset > 0) must finish before any Response or Event starts. Then that route is deferred. |
| Default | `flush` Pending or timeout on a terminal envelope is adapter pressure, not connection death. Do not `close_all()` / `WriteFailure` the Unix connection for that case. |
| Default | IsolatedHub proof must record one Status Response after flood pressure starts and before `core_adapter_closed`. Post-close Status alone is not the host-readable oracle. |
| Default | Sibling proof is new terminal envelopes after the close, plus a content-blind bound Drain that remains owned. `ListSessions` lifecycle alone is not enough ([[mux envelope delivery does not prove Hub route ownership]]). |
| Non-goal | Test-only `force_would_block` as the live IsolatedHub oracle. That helper stays on the in-process harness. |
| Non-goal | Optional flush quotas, runtime flags, or a HoldFlush daemon request. |
| Follow-up-ok | TUI retargets its live test onto this IsolatedHub recipe after this ticket merges. |
| Ask-human | Only if Implement discovers Core `f4f6bf5` does not close the adapter after 512 in-flight ticks on the production Unix adapter. Current Core tests say it does. |

## Implementation plan

### Why the shipped test is not this ticket

`core_write_budget_hard_stop_emits_core_adapter_closed` already binds `yes write-budget-stall` and `sleep 30` on one connection and asserts `core_adapter_closed`. Production `flush_unix_mux_writes` still:

1. Resumes whatever pending frame is in flight (often a flood envelope).
2. Serializes every occupied terminal slot before any host Event.
3. Treats a later write `Err` as connection death and `close_all()`.

`UnixConnectionMux::close_all` sets `dying` and `close_from_host()` on every route. The Event reason then becomes `host_adapter_closed`. That is the TUI observation.

### Surgical production change

Keep the one-slot adapter. Change the connection writer, including the Response path:

1. **One ordered `MuxWriteState` for all three mux classes.** `DaemonResponse`, `DaemonEvent`, and `DaemonUnixTerminalEnvelope` serialize through the same pending bytes + offset writer. Production entry: `handle_connection_async` after `receive_control_response` must not call `write_async_frame(&mut write_half, &response)`. Enqueue the Response into `mux_write` and flush it there. HelloAck may keep the existing one-shot write because mux write state does not exist yet.
2. **Delivery ack and close-after ride on the queued Response.** Extend `PendingMuxFrame` with `delivery_ack: Option<mpsc::Sender<()>>` and `close_after: bool`. Only `DaemonShutdown` and `StartHubUpdate` set `delivery_ack` (same `requires_delivery_ack` predicate as today). Only `DaemonShutdown` sets `close_after`. `resume_pending_mux_write` sends the ack **after** `MuxWrite::Written` for that Response, never when offset is partial and never before the first write attempt completes the line. If the Response write errors, do not ack success; drop the sender so `wait_for_response_delivery` unblocks the same way today's owner-drop test already requires.
3. **`close_after_response` stays in the write loop.** After enqueueing a `close_after` Response, keep calling `flush_unix_mux_writes` until that frame is Written or `DAEMON_CLIENT_WRITE_TIMEOUT` elapses. Do not `return Ok(())` and do not `mux.close_all()` while that Response remains pending. After Written, set `ConnectionTerminalReason::NormalClose` and return. Timeout uses the existing write-failure path. This replaces the current "write_all then immediately return" sequence without unbounded `block_on`.
4. **Partial-line rule before a Response.**
   - pending terminal offset == 0: abandon that serialization, leave `complete_active()` uncalled, then write the Response.
   - pending terminal offset > 0: resume that JSON line to completion (50ms bounded writes), then write the Response.
   - pending host Event: finish it, then write the Response.
   Never splice Response bytes into a terminal line.
5. **Host-first flush.** After the pending frame that must finish, write queued Responses and `DaemonEvent`s before new `snapshot_writes`. `TerminalSubscriptionClosed` is a host Event and must not sit behind a flood.
6. **Terminal Pending is not mux death.** `write_frame_bytes_resumable` already returns `MuxWrite::Pending` on timeout or WouldBlock. Do not promote that to `close_all()`.
7. **Zero-progress terminal start is abandonable.** If a newly started terminal envelope writes 0 bytes before Pending, drop that pending serialization and leave `complete_active()` uncalled. The slot stays `Full`. Core's in-flight 512-tick budget can fire.
8. **Defer that route after backpressure.** Skip further `snapshot_writes` for a handle that just abandoned or finished a backpressured frame, so one flood cannot re-occupy the single pending slot. Sibling handles still flush.
9. **Keep `queue_unix_subscription_closed_events` on the control thread.** Reason remains `host_closed` vs not. Do not add a third reason.

Do not decode terminal bodies. Do not add a second policy queue. The adapter remains one in-flight slot. Keep the 50ms write bound; do not restore `write_all` with the 2s `DAEMON_CLIENT_WRITE_TIMEOUT` for mux frames.

### IsolatedHub proof TUI can consume

Replace or tighten `core_write_budget_hard_stop_emits_core_adapter_closed` in `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`. Drive the real Hub binary through `botster-hub-client` only:

1. Hello + Attach two sessions on **one** Unix connection: flood command (`yes …`) and a live sibling that can echo (`while read` / `printf` loop).
2. After pressure starts (first flood terminal envelope **or** 200ms elapsed, whichever comes first) and **before** any `TerminalSubscriptionClosed`, send `Status` on the same connection. Keep reading. Record that Response. A Status that only happens after close fails this check.
3. Loop until close or deadline:
   - Keep calling `read_unix_mux_frame_from_reader`.
   - Never stop reading to create pressure.
   - Collect Event and Terminal frames. Do not wait for a read timeout before the pre-close Status.
4. Assert the stall session emits exactly one `TerminalSubscriptionClosed` with `reason == TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER`.
5. Assert no `host_adapter_closed` for that identity.
6. Assert `Status` and `ListSessions` succeed on the same connection after the Event (second host oracle).
7. SendInput to the sibling and wait for a **new** sibling terminal envelope. Also issue a content-blind scoped Drain that stays owned.
8. Record IsolatedHub binary provenance: Hub checkout SHA `aafd6c2` plus this ticket's merge SHA, locked Core worker `f4f6bf5`.

Ablations (each must redden at its own assertion; do not let the first failure vouch for later ones):

| Ablation | Expected red |
| --- | --- |
| Restore `write_async_frame` beside a nonzero-offset pending terminal line | mux parse fails or Response is not a complete line |
| Fire `response_delivery_tx` before the shutdown/update Response line completes | `wait_for_response_delivery` returns while the client has no complete Response |
| Return / `close_all` while a `close_after` Response is still pending | shutdown Response never appears on the mux |
| Restore terminal-before-host flush and `close_all` on terminal Pending | `host_adapter_closed` or Status unreadable |
| Drop the pre-close Status assertion | post-close Status alone would still pass |
| Drop the exact-reason assertion | would accept `host_adapter_closed` |
| Drop sibling SendInput / envelope wait | `ListSessions` running would still pass |

Keep existing host-close, Detach, connection-death, process-exit, and stale-generation tests. They remain the late-message matrix.

### Docs

Update `docs/client-protocol.md` TerminalSubscriptionClosed paragraph: host Response/Event remain readable on the same connection; a sibling adapter may keep writing terminal envelopes; `host_adapter_closed` is not the Core write-budget oracle.

## Runtime-teardown lens answers

| Field | Content |
| --- | --- |
| `teardown_class_applies` | **yes**. Ticket is Unix mux / adapter / ClientWorker write-budget teardown with sibling isolation and terminal-state vs live-runtime divergence (`host_adapter_closed` vs Core close). |
| `teardown_isolation` | Ownership set that dies: one mux route `(client_id, session_id, subscription_id, generation)` plus that handle's one-slot adapter. Healthy sibling route on the same connection stays bound and writable. `close_all()` remains only for true connection death (EOF, unrecoverable write, shutdown), not for one route's Full slot. |
| `teardown_bounds` | Adapter `close()` stays non-blocking. All mux classes use 50ms cancellation-safe `write` with byte offset. A Response waits only to finish an already-started JSON line, then writes. `DaemonShutdown` / `StartHubUpdate` wait for their Response line at most `DAEMON_CLIENT_WRITE_TIMEOUT` (2s). Terminal Pending does not `block_on` the control thread and does not `close_all`. Core's hard stop is still 512 host ticks then synchronous `close()` + drop. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | IsolatedHub Unix path: flood session occupies one adapter slot → Hub defers that route instead of filling/killing the mux → same connection writes a Status Response through the ordered mux writer while that adapter is still open → Core 512-tick `close()` (`host_closed == false`) → control thread `queue_unix_subscription_closed_events` → connection loop writes mux-classified `DaemonEvent::TerminalSubscriptionClosed` reason `core_adapter_closed` → Status and sibling terminal frames continue. Oracles: pre-close Status Response, exact reason, post-close Status, sibling envelope, Hub SHA + locked Core SHA. A JSON fixture or in-process `force_would_block` is not the live proof. |
| `ownership_identity` | Live owner is Core `(session_id, subscription_id, generation)` plus Hub mux route for that generation. Event for N must not close N+1. Same-connection sibling uses a different `session_id`. Existing two-connection stale-generation test stays. |
| `sibling_fail_closed_policy` | Success: sibling keeps delivering terminal frames; host control stays up. Ultimate write failure of the **connection** still `close_all()` and does not emit `TerminalSubscriptionClosed` (dying). That blast radius is the connection, not one route. Test the success path (sibling lives). Connection-death test already covers fail-closed. |

### Late-message matrix

| Message | Grant / owner tag | Reject after this failure | Residual sweep |
| --- | --- | --- | --- |
| Hello / terminal admission | connection `UnixTerminalAdmission` | unchanged | none |
| Attach | client + session + subscription | `OperatorError` if Rejected; same-connection re-attach of one key still fail-closes | no new Core owner |
| SendInput / Resize | session + live bound generation | generation / gone | must not revive the closed adapter |
| Detach | session + subscription + live generation | `AlreadyGone` | no `TerminalSubscriptionClosed` |
| Status / ListSessions | host control | never rejected because one adapter is Full or closed | must succeed on the same connection **while the stalled adapter is still open**, and again after close |
| Drain (scoped) | route ownership | closed route is gone; sibling Drain stays owned | Hub-visible ownership oracle |
| Terminal slot `try_write` | adapter handle | `Full` / `WouldBlock` while deferred; `Closed` after Core hard-stop | Core drops adapter on the tick |
| `TerminalSubscriptionClosed` for N after N+1 live | generation | do not emit for N+1 | must not close N+1 |
| Connection EOF / unrecoverable write | `client_id` | `close_all`, dying | no `TerminalSubscriptionClosed` |
| ShutdownSession / process exit | host session id | lifecycle / `ProcessExit` only | suppress Event |
| `DaemonShutdown` | connection | ack after Response line Written; wait ≤ 2s | do not `close_all` while Response pending; then NormalClose |
| `StartHubUpdate` | connection | ack after Response line Written; wait ≤ 2s | handoff waits on the same `response_delivery_rx` as today |

## Assumptions and unknowns

- Assumption: Core `f4f6bf5` counts an accepted Unix-adapter write that never calls `complete_active()` toward the 512-tick budget. Core unit tests say yes. IsolatedHub must prove it on the production adapter.
- Assumption: same-connection sibling is possible once Hub withholds the stalled route's socket writes. Ticket allows a second connection only if that is impossible; this plan makes same-connection the required path.
- Assumption: no DTO change is required. TUI already decodes `TerminalSubscriptionClosed` and the two reason constants.
- Unknown until Implement: how quickly IsolatedHub's control thread pumps Core versus the 25ms flush wake. The test deadline must cover 512 Core ticks plus emit, not a guessed 2s sleep.
- Unknown: whether `yes` plus keep-reading can still fill a large socket buffer before the first Pending. If CI is too fast and never backpressures, Implement holds the slot by abandoning zero-progress writes under the existing 50ms timeout rather than adding a knob.

## Affected surfaces/files

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | Route Responses through `MuxWriteState`; attach delivery_ack and close_after to the pending Response; ack only after Written; host-first flush; terminal Pending ≠ `close_all`; abandon zero-progress terminal start; finish nonzero-offset lines before Response |
| `src/unix_terminal_adapter.rs` | Route defer / skip-flush on the handle if the writer needs a flag; do not change close-reason rules |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Tighten write-budget test: pre-close Status, keep-reading, exact `core_adapter_closed`, live sibling frames |
| `src/daemon_transport.rs` mux_write_resume_tests | Partial terminal line + Response + resume parses two complete mux lines; flush-order and zero-progress abandon |
| `src/daemon_transport.rs` existing shutdown/update tests | Add partial-terminal-then-`DaemonShutdown` / `StartHubUpdate` proofs: ack after complete Response line; connection does not close while pending |
| `docs/client-protocol.md` | Host-readable / sibling-live / not-host-oracle wording |
| `docs/plans/emit-core-adapter-closed-while-unix-host-mux-stays-readable.md` | this plan |
| `docs/reports/emit-core-adapter-closed-while-unix-host-mux-stays-readable-implement.md` | Implement report (later) |

No `Cargo.lock` / protocol version / hub-test-support version change expected.

## Risks

- Partial JSON line on the socket: abandoning a frame that already wrote bytes corrupts the mux. Only abandon at 0 bytes; resume partials **before** any Response.
- `write_async_frame` today bypasses `mux_write.pending`. Leaving that call in `handle_connection_async` would still splice Status into a flood line even if flush order is fixed.
- Enqueueing a shutdown Response without carrying `response_delivery_tx` / `close_after` can start the update handoff or drop the shutdown line. Ack only after Written; wait ≤ 2s.
- Host-first flush could starve snapshot delivery if Events are unbounded. Keep the existing pending-event pop; do not add a new queue. Events here are close notifications, not PTY chatter.
- Deferring a flood route might look like Hub dropping terminal bytes. That is the adapter Full contract: Core owns the drop/close, Hub just stops completing the slot.
- Existing 8s wait may be too short once the client keeps reading and isolation delays first Pending. Measure against 512 Core ticks; raise the deadline if the production pump is slower, do not fake the close.
- Downstream TUI still has its own mux-read bugs (`finding_1786715974_898936`). This ticket does not fix TUI framing. The Hub proof must be consumable by `read_unix_mux_frame_from_reader` on a live reader.

## Acceptance checks/tests

Production entry points that must use the new behavior:

- Unix connection loop in `handle_connection_async`: Response writes go through `MuxWriteState`, not `write_async_frame`
- `DaemonShutdown` / `StartHubUpdate` ack after the Response JSON line is Written; close-after waits in the flush loop
- `flush_unix_mux_writes` host-first flush and route defer
- Control thread `queue_unix_subscription_closed_events` → `UnixConnectionMux::queue_closed_subscription_events`
- Core ClientWorker 512-tick hard-stop → adapter `close()` without `host_closed`

`./test.sh --offline --test hub_daemon_lifecycle_test unix_adapter` is **not** an acceptance command. Plan Review executed it independently: 8 passed, 178 filtered. It does not run `core_write_budget_hard_stop_emits_core_adapter_closed` or the other named close tests. The last token is a cargo name filter, not a module selector.

Commands (each required close test by exact name):

```sh
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact core_write_budget_hard_stop_emits_core_adapter_closed
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact host_adapter_close_emits_terminal_subscription_closed_for_one_route
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact connection_death_and_detach_do_not_emit_terminal_subscription_closed
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact stale_generation_close_does_not_sweep_replacement_owner
./test.sh --offline --test hub_daemon_lifecycle_test -- --exact failed_remove_session_does_not_suppress_later_core_close
cargo test --offline -p botster-hub --lib mux_write_resume_tests
cargo test --offline -p botster-hub --lib daemon_shutdown_waits_for_response_delivery_before_stopping
cargo test --offline -p botster-hub --lib daemon_shutdown_releases_when_delivery_owner_drops
cargo test --offline -p botster-hub-client --lib
cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
```

Add focused tests (names may be exact or under `mux_write_resume_tests`):

- partial terminal line, then `DaemonShutdown` Response: two complete mux lines; `response_delivery_tx` fires only after the Response newline; connection loop does not return while pending
- partial terminal line, then `StartHubUpdate` Response: same ack-after-Written rule; control-thread `wait_for_response_delivery` does not observe the signal early

Implement report must record executed vs filtered counts for each command. Optional suite smoke: `./test.sh --offline --test hub_daemon_lifecycle_test` with no name filter (full target; do not append `unix_adapter`).

IsolatedHub oracles (live path, not a fixture):

- One Status Response after pressure starts and before `core_adapter_closed`
- Exact reason `core_adapter_closed`
- Status readable after the close
- Sibling terminal envelopes after the close
- Content-blind sibling Drain still owned
- Hub checkout SHA and lockfile Core SHA `f4f6bf5babe92dfb9241a760c414187f711c2c42` recorded
- Ablation of `write_async_frame` beside a partial terminal line reddens the ordered-line unit test
- Ablation of host-first / no-`close_all` reddens the reason or Status oracle

Downstream:

- Do not edit TUI.
- If `botster-hub-client` public API is unchanged, TUI scratch Cargo check is optional confirmation only.
- If a public helper is added, scratch-patch TUI `cargo check --workspace` against this worktree.

Merge directly into `main`. Do not create a PR.

## Vault gaps worth capturing

- Unix mux single pending frame can serialize host Events behind a stalled terminal write, so socket pressure becomes `host_adapter_closed` via `close_all`.
- `write_async_frame` for Response bypasses that pending line and can split a terminal JSON frame.
- `ListSessions` lifecycle `running` is not sibling terminal delivery and is not Hub route ownership.
- `host_adapter_closed` after a client stall is host egress pressure, not Core's 512-tick write-budget.
- A cargo filter token that looks like a module name (`unix_adapter`) can exclude the named close tests.
- Enqueued mux Responses still owe `StartHubUpdate` / `DaemonShutdown` their post-write delivery ack and close-after sequencing.

Capture after Implement if the flush-order / abandon rule ships and is not already covered by [[Unix mux host events are unsolicited control frames]].
