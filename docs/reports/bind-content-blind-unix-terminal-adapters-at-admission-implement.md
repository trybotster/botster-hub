# Implement report: Bind content-blind Unix terminal adapters at admission

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | this run's Hub worktree |
| Ticket | `ticket_1786661008_634435` |
| Run | `run_1786681880_322827` |
| Step | `botster_stack_implement` (`run_step_1786701773_868027`, Review-loop 5) |
| Approved plan | `docs/plans/bind-content-blind-unix-terminal-adapters-at-admission.md` revision 6 |
| Merge policy | direct (no PR) |
| Locked Core SHA | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and the Plan artifact both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — public `stream_attach` helper
- [[botster playbooks compose role with changed surface overlays]] — runtime surface plus class overlay
- [[botster runtime teardown lenses]] — required; class applies

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[hub daemon runtime stays on one owner thread while socket handlers submit requests]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[session wide drains cannot deliver subscription owned initial state]]
- [[attach routes use subscription scoped Core drains]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[adding a hub client feature constant is a three site change]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[attach failed cleanup is route aware and idempotent]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[cli-patterns]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — package/plugin workflow implementation is out of scope
- Other repository charters (Core, Web, TUI, Workspaces, Ghostty)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved revision-6 plan; keep Hub charter ownership
- Do not implement Core, WebRTC adapter, cold-cut, Web, TUI, or Project Pipelines
- Use `./test.sh` / `BOTSTER_ENV=test` wrappers
- Advertise `unix_terminal_adapter` without raising `DaemonCompatibilityRequirement::current()`
- `PROTOCOL_VERSION` stays 7; bump `CONFORMANCE_FIXTURE_REVISION` 38 → 39
- Runtime-teardown lenses are implemented, not deferred

## Files changed

| Path | Change |
| --- | --- |
| `Cargo.toml` / `Cargo.lock` | Pin Core git deps to `f4f6bf5`; add `botster-terminal-protocol` |
| `src/unix_terminal_adapter.rs` | Production one-slot adapter, mux, Core harness driver |
| `src/lib.rs` | Private `unix_terminal_adapter` module |
| `src/runtime.rs` | `bind_terminal_adapter` / `list_terminal_subscriptions` / `detach_terminal_subscription` facades; `BindTerminalAdapter` error class |
| `src/daemon_attach_stream.rs` | Generation + bound flags; connection-bound ledger stores owner+generation; `cancel_stream` forgets the key and closes the adapter; cleanup match is owner+generation |
| `src/daemon_transport.rs` | Hello admission, Attach bind sequence, bound Drain filter, bound disconnect close-only; cleanup mutates a route only when the closing client still owns that generation |
| `src/config.rs` | Default fields required by the Core pin |
| `src/main.rs` | Unbound smoke uses Attach + scoped Drain + `ReadScreen` after the Core pin |
| `crates/botster-hub-client/src/lib.rs` | Optional feature, mux types, `for_unix_terminal_adapter()`, revision 39 |
| `crates/botster-hub-client/src/typescript.rs` + generated TS | Feature + envelope types |
| `docs/client-protocol.md` / `README.md` | Optional Unix adapter plane; revision 39 |
| `packages/hub-test-support/*` | Version 0.1.33 → 0.1.34; regenerated fixtures; `test.mjs` asserts revision 39 |
| `crates/botster-hub-test-support/src/lib.rs` | Published conformance uses `DaemonConnection` Attach + scoped Drain; docs no longer claim that runner calls `stream_attach` |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | IsolatedHub bind/detach/unbound proofs; stale A-disconnect after B bind; live `stream_attach` late-byte completion |
| `tests/hub_daemon_lifecycle/sessions.rs` | Fast-exit, late-history, mode flags, entity, shutdown consumers use scoped Drain + `ReadScreen` |
| `tests/hub_daemon_lifecycle/shutdown.rs` | Restart recovery attaches, drains, and reads the screen on the same connection |
| `tests/hub_daemon_lifecycle/session_fixtures.rs` / `webrtc_proofs.rs` | Shared helpers drain the bound/unbound route before ReadScreen/mode flags; WebRTC uses scoped Drain |
| `tests/hub_test_support_conformance_test.rs` | Incremental history frames collapse to attaching → history → attached → live |
| `tests/hub_runtime_test.rs` / `tests/hub_capability_runtime_test.rs` | Scoped drain + `ReadScreen` after Core pin |
| `docs/plans/bind-content-blind-unix-terminal-adapters-at-admission.md` | Approved revision-6 plan; Authoritative path is spawn-target wording (no user home path) |
| `docs/reports/bind-content-blind-unix-terminal-adapters-at-admission-implement.md` | This report |

## Ownership boundaries preserved

- **Hub owns:** Unix Hello + `LocalOperator` admission, route records, adapter instance, mux framing, transport write, HubRuntime facades
- **Core owns:** queues, attach phases, bind/inventory, capability set, mechanical detach, harness
- **hub-client owns:** optional feature constant, mux DTO, request-specific requirement
- **Not touched:** Core source, WebRTC adapter, Web/TUI decoders, Project Pipelines package, host session shutdown policy

## Cross-repo dependencies or separately routed work

| Item | Status |
| --- | --- |
| Core ClientWorker push `ticket_1786661004_845807` | Closed parent |
| Core capability-bind `ticket_1786682902_405026` | Closed; consumed at `f4f6bf5` |
| WebRTC adapter `ticket_1786661008_247079` | Separate ticket; not implemented |
| Cold-cut `ticket_1786661010_198387` | Separate ticket; one-frame Attaching exception and Drain translation remain |

## Deviations from plan

1. **Unbound smoke no longer uses `stream_attach`.** After the required Core pin, a fast-exit `printf` attach still returns only `Attaching` then Snapshot/`Attached` on Drain. Visible text is on `ReadScreen`. `ProcessExit` does not appear on unbound Drain, and host lifecycle stays `running`. `stream_attach`'s ProcessExit/`exited` wait never completes. Production `hub smoke` now does Attach + scoped Drain until `Attached` + `ReadScreen` + `ShutdownSession`. This is the unbound visible-state path required by the Core pin, not a new live-output dual path.
2. **Fast-exit diagnostic accepts `ReadScreen`.** Same Core-pin contract. Classification remains `output_produced_not_routed` when Drain has no `TerminalOutput`.
3. **CLI and conformance attach proofs use `ReadScreen` instead of blocking on `stream_attach`.** `sessions attach` still streams via `stream_attach`, which now also restores `ReadScreen` after Snapshot/`Attached`. Tests that previously waited for `ProcessExit` on unbound attach now prove visible state through `ReadScreen` so the suite cannot hang after the Core pin.
3. **Control-loop `try_recv` before `block_on`.** `RegisterUnixAdmission` is an extra control message on every Hello. Immediate `try_recv` keeps request handling from waiting on the reconcile timer. First reconcile stays `Instant::now()` (no delayed first tick).
4. **No IsolatedHub injection of unexpected pre-bind Snapshot/ProcessExit.** Fail-closed is implemented and unit-tested (`initial_attaching_only`, `fail_closed_pre_bind_attach`). IsolatedHub cannot force Core to return extra attach egress without a Core test hook.

Plan acceptance checks that remain true: bind only when Hello requires the feature; one-frame Attaching exception; fail-closed on any other pre-bind terminal event; bound Drain has no terminal bodies; connection death closes adapter only; explicit Detach is separate; host session stays listed; harness passes; feature is optional.

## Review findings in this Implement loop

| Finding | Resolution |
| --- | --- |
| Bound socket death took Hub Detach | Snapshot bound keys before close. Connection-scoped bound routes survive later adapter-flag clears. Bound death does not increment `cleanup_hub_detach`. Explicit Detach increments `explicit_detach`. IsolatedHub asserts that delta. |
| Required workspace suite failed | Integrated `origin/main` `d92aace`. `./test.sh --locked` exits 0. |
| `test.mjs` still asserted revision 38 | All four asserts are 39. |
| Plan leaked an absolute user path | Authoritative path is spawn target `botster-hub`. |
| Unbound Drain lost TerminalOutput | Unbound scoped Drain still translates Snapshot, later TerminalOutput, and ProcessExit. Tests that wait for live bytes now collect scoped Drain events. They do not treat ReadScreen as a Drain oracle. Public conformance collects TerminalOutput from the same scoped Drain request. |
| Disconnect test did not prove omitted Detach | Status `cleanup_by_reason` records `bound_adapter_close`, `cleanup_hub_detach`, and `explicit_detach`. Bound disconnect asserts `cleanup_hub_detach` delta is 0. Explicit Detach asserts `explicit_detach` is 1. |
| Stale connection-bound keys can cancel a replacement owner | Ledger entries store session, subscription, and generation. `cancel_stream` forgets the key on Detach, replacement Attach, reconcile, and fail-closed. Cleanup cancels or Detaches a route only when the current owner and generation still match the closing connection. IsolatedHub: A binds, A detaches and stays connected, B binds the same key, A disconnects, B still receives `echo:after-a-drop`. |
| Claimed stream_attach conformance path does not call stream_attach | Published conformance stays on `DaemonConnection` Attach + scoped Drain. Docs state that split. `unix_adapter_unbound_stream_attach_returns_late_bytes` calls `botster_hub_client::stream_attach`, produces output after Attached, exits the process, and asserts the helper returns the late bytes and completes. |
| Live stale-owner test stayed green without ledger guards | Mux envelopes are not the owner oracle. After A disconnects the test asserts `bound_adapter_close` delta is 0, then requires B's scoped Drain to stay free of Snapshot/TerminalOutput/ProcessExit while opaque envelopes continue. `cancel_stream` now closes the adapter so a stale cancel cannot leave Core writing to a dropped Hub route. Ablation: `forget_connection_bound_route` no-op plus `connection_bound_route_still_owned` always true reddens at `A's disconnect must not close B's bound route` (`bound_adapter_close` 1 vs 0). Worktree restored after the probe. |

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One Unix subscription owns one adapter, route record, and Core generation. Close/fail tears down only that key. IsolatedHub disconnect leaves the host session listed. |
| Bounded teardown | `try_write` / `close` / `Drop` are non-blocking and lock-free for the writer. Core harness plus `close_does_not_wait_on_occupied_slot` prove this. No `block_on(close)` on the owner thread. |
| Late-message matrix | Attach fail-closes unexpected pre-bind frames. Bind rejects stale/unknown/already-bound. Bound EOF/write failure closes adapter only. Explicit Detach is authorized. Drain filters terminal bodies on bound routes. Inventory reconcile drops Hub rows Core no longer reports. WebRTC grant path unchanged. |
| Production-path hard-stop | IsolatedHub: Hello + feature → Attach only `Attaching` → opaque envelopes → bound Drain has no terminal bodies → drop connection → session still listed. Provenance printed from the live Hub binary and session-worker path. |
| Ownership identity | Route key is `(client_id, session_id, subscription_id, generation)`. The connection-bound ledger stores that generation. Every cancel path forgets the key. A delayed disconnect mutates the live route only when owner and generation still match. Unix `grant_id` is `None`. |
| Sibling / fail-closed | Adapter close does not call `ShutdownSession`. Explicit Detach is a separate IsolatedHub test. Unbound printf stays `running` after process exit. |

## Tests and downstream proof run

Commands (all with `BOTSTER_ENV=test`):

- `./test.sh --locked --offline --test hub_daemon_lifecycle_test unix_adapter` — IsolatedHub bind, detach, unbound Snapshot, feature floor
- IsolatedHub `unix_adapter_stale_disconnect_does_not_cancel_replacement_owner` — after A disconnects, `bound_adapter_close` delta is 0, B's scoped Drain has no terminal bodies, and opaque envelopes still carry `echo:after-a-drop`
- IsolatedHub `unix_adapter_unbound_stream_attach_returns_late_bytes` — public `stream_attach` returns `late-stream-attach` and completes
- IsolatedHub `unix_adapter_unbound_printf_stream_attach_completes` — `ReadScreen` has the smoke marker; host session stays `running`
- `fast_exit_attach_diagnostic_records_subscription_event_order` — official diagnostic; `read_screen_marker=true`
- `./test.sh --locked --offline --test hub_runtime_test bind_terminal_adapter_inventory_echoes_capability_set`
- `./test.sh --locked --offline --lib unix_terminal` — Core harness + close-without-wait + content-blind source scan
- `./test.sh --locked --offline -p botster-hub-client unix_terminal` — optional feature does not raise the default requirement
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` — exit 0
- `cargo fmt --all -- --check` — exit 0
- Clean `npm install --omit=dev && npm test` in a temp copy of `packages/hub-test-support` — exit 0
- Plan/report leak scan vs a known-positive absolute home-path control — plan and report have no user home paths
- `./test.sh --locked` — workspace wrapper exit 0. `hub_daemon_lifecycle_test`: 176 passed, 0 failed, 1 ignored. Other workspace members and doctests passed.

Production entry points:

- `handle_connection_async` → Hello → `RegisterUnixAdmission` → `DaemonRequest::Attach` → `bind_unix_adapter_after_attaching` → `HubRuntime::bind_terminal_adapter`
- Bound later frames: owner-thread `pump_bound_unix_routes` + connection `flush_unix_adapter_envelopes`
- Unbound smoke: `smoke_session_round_trip` Attach/Drain/`ReadScreen`

## Unverified behavior or residual risk

- `stream_attach` now has a live IsolatedHub proof for the sleep-then-print-then-exit path. It still completes only on `ProcessExit`, `attach_failed`, or host `exited`. A long-running session that never exits still streams. A fast-exit session whose child is already dead during attach may stay attached until the caller detaches; smoke and published conformance still use Attach + scoped Drain + `ReadScreen` for that case.
- Unexpected pre-bind fail-closed is unit-tested, not IsolatedHub-injected.
- Inventory capability echo is an in-process `HubRuntime` proof, not a Unix socket DTO (no public inventory request).
- WebRTC adapter path is unchanged and unclaimed. WebRTC proofs still use the unbound Drain path with scoped subscription drains.
- After the Core pin, entity lifecycle patches still require a terminal Drain to advance the Core journal. Tests drain while they wait. Hub does not add an `observe_lifecycle` facade because Core `f4f6bf5` does not export that method.

## Missing vault guidance discovered

- No note stating that after ClientWorker push + capability bind, unbound fast-exit attach delivers visible text only through `ReadScreen` and does not emit `ProcessExit` or host `exited` on Drain. Worth capturing after this run.
- `observe_lifecycle` is documented as the Hub lifecycle progress operation, but Core `f4f6bf5` does not export that method. Drain still calls `reconcile_lifecycle_observations`. No Hub facade was added.
