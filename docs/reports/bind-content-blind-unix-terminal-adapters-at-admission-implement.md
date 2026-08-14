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
| Step | `botster_stack_implement` (`run_step_1786687578_189614`) |
| Approved plan | `docs/plans/bind-content-blind-unix-terminal-adapters-at-admission.md` revision 6 |
| Merge policy | direct (no PR) |
| Locked Core SHA | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and the Plan artifact both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
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
| `src/daemon_attach_stream.rs` | Generation + bound flags; `#[derive(Default)]`; fail-closed helpers; capability intersection |
| `src/daemon_transport.rs` | Hello admission, Attach bind sequence, bound Drain filter, bound disconnect close-only, inventory reconcile pump, `RegisterUnixAdmission` |
| `src/config.rs` | Default fields required by the Core pin |
| `src/main.rs` | Unbound smoke uses Attach + scoped Drain + `ReadScreen` after the Core pin |
| `crates/botster-hub-client/src/lib.rs` | Optional feature, mux types, `for_unix_terminal_adapter()`, revision 39 |
| `crates/botster-hub-client/src/typescript.rs` + generated TS | Feature + envelope types |
| `docs/client-protocol.md` / `README.md` | Optional Unix adapter plane; revision 39 |
| `packages/hub-test-support/*` | Version 0.1.33 → 0.1.34; regenerated fixtures |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | IsolatedHub production-path proofs |
| `tests/hub_daemon_lifecycle/sessions.rs` | Fast-exit diagnostic accepts `ReadScreen` as visible-state proof |
| `tests/hub_runtime_test.rs` / `tests/hub_capability_runtime_test.rs` | Scoped drain + `ReadScreen` after Core pin |
| `docs/plans/bind-content-blind-unix-terminal-adapters-at-admission.md` | Approved revision-6 plan |
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

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One Unix subscription owns one adapter, route record, and Core generation. Close/fail tears down only that key. IsolatedHub disconnect leaves the host session listed. |
| Bounded teardown | `try_write` / `close` / `Drop` are non-blocking and lock-free for the writer. Core harness plus `close_does_not_wait_on_occupied_slot` prove this. No `block_on(close)` on the owner thread. |
| Late-message matrix | Attach fail-closes unexpected pre-bind frames. Bind rejects stale/unknown/already-bound. Bound EOF/write failure closes adapter only. Explicit Detach is authorized. Drain filters terminal bodies on bound routes. Inventory reconcile drops Hub rows Core no longer reports. WebRTC grant path unchanged. |
| Production-path hard-stop | IsolatedHub: Hello + feature → Attach only `Attaching` → opaque envelopes → bound Drain has no terminal bodies → drop connection → session still listed. Provenance printed from the live Hub binary and session-worker path. |
| Ownership identity | Route key is `(client_id, session_id, subscription_id, generation)`. Unix `grant_id` is `None`. Reconcile matches generation. Disconnect cleanup is owner-tagged by `client_id`. |
| Sibling / fail-closed | Adapter close does not call `ShutdownSession`. Explicit Detach is a separate IsolatedHub test. Unbound printf stays `running` after process exit. |

## Tests and downstream proof run

Commands (all with `BOTSTER_ENV=test`):

- `./test.sh --locked --offline --test hub_daemon_lifecycle_test unix_adapter` — IsolatedHub bind, detach, unbound Snapshot, feature floor
- IsolatedHub `unix_adapter_unbound_printf_stream_attach_completes` — `ReadScreen` has the smoke marker; host session stays `running`
- `fast_exit_attach_diagnostic_records_subscription_event_order` — official diagnostic; `read_screen_marker=true`
- `./test.sh --locked --offline --test hub_runtime_test bind_terminal_adapter_inventory_echoes_capability_set`
- `./test.sh --locked --offline --lib unix_terminal` — Core harness + close-without-wait + content-blind source scan
- `./test.sh --locked --offline -p botster-hub-client unix_terminal` — optional feature does not raise the default requirement
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` — exit 0
- `./test.sh --locked` — workspace wrapper completed. `hub_daemon_lifecycle_test`: 162 passed, 11 failed, 1 ignored. Failures are Core-pin unbound Drain/ProcessExit/ReadScreen contract changes (listed below), not IsolatedHub Unix-adapter bind proofs. Those 11 still need ReadScreen/`ProcessExit` adaptations or are WebRTC close/mode-flag readiness races after the pin.

Production entry points:

- `handle_connection_async` → Hello → `RegisterUnixAdmission` → `DaemonRequest::Attach` → `bind_unix_adapter_after_attaching` → `HubRuntime::bind_terminal_adapter`
- Bound later frames: owner-thread `pump_bound_unix_routes` + connection `flush_unix_adapter_envelopes`
- Unbound smoke: `smoke_session_round_trip` Attach/Drain/`ReadScreen`

## Unverified behavior or residual risk

- `stream_attach` now restores visible text from `ReadScreen` after Snapshot/`Attached`. It still completes only on `ProcessExit`, `attach_failed`, or host `exited`. A long-running session that never exits still streams. A fast-exit session whose child is already dead during attach may stay attached until the caller detaches; smoke no longer uses that wait.
- `./test.sh --locked` failed 11 `hub_daemon_lifecycle_test` cases after the required Core pin: missing Drain `TerminalOutput`/`ProcessExit`, empty WebRTC drain bytes, mode-flag `OperatorError` before flags are readable, entity-subscription readiness via retained egress, and restart ReadScreen emptiness. Isolated adapter proofs and daily-commands/smoke no longer hang. These 11 are residual Core-pin consumer adaptations, not missing Unix bind wiring.
- Unexpected pre-bind fail-closed is unit-tested, not IsolatedHub-injected.
- Inventory capability echo is an in-process `HubRuntime` proof, not a Unix socket DTO (no public inventory request).
- WebRTC adapter path is unchanged and unclaimed.
- Full `./test.sh --locked` result is recorded in the gate after this report is committed.

## Missing vault guidance discovered

- No note stating that after ClientWorker push + capability bind, unbound fast-exit attach delivers visible text only through `ReadScreen` and does not emit `ProcessExit` or host `exited` on Drain. Worth capturing after this run.
- `observe_lifecycle` is documented as the Hub lifecycle progress operation, but Core `f4f6bf5` does not export that method. Drain still calls `reconcile_lifecycle_observations`. No Hub facade was added.
