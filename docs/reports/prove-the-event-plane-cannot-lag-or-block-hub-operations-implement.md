# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787377118_173762`) |
| Approved plan | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` revision 8 |
| Merge policy | `direct` into `main`; do not create a PR |
| Integrated base | `origin/main` `baeb04d` after merge into this ticket branch |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: `project_pipelines_current_context` was unavailable because the Botster MCP handshake failed in this session. Ticket and run rows in the Project Pipelines database, plus `BOTSTER_TARGET_ID`, both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

### Targeted atomic notes

- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[event plane client proof uses library contract fixtures]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[hub client event queue max requires Botster test mode]]
- [[test script required for rust tests not cargo test]]
- [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[Client event holders are connection-scoped]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[web event plane budgets are published numeric host limits]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[event plane terminal budgets are new coexistence regression budgets]]

`project-pipelines-playbook` was not loaded for package/plugin path edits. This run did not change Project Pipelines sources.

## Files changed

| Path | Change |
| --- | --- |
| `docs/event-plane-load-proof.md` | published budget contract, machine profile, formulas, verdicts |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json` | calibration dataset placeholder; thresholds remain null until the reference-runner dispatch |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | this report |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | machine-readable evidence shape with executed vs cited revisions |
| `crates/botster-hub-test-support/src/lib.rs` | `run_client_event_conformance` at the public Unix host-control boundary |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | source guards, isolated client-event proof, ignored two-arm campaign |
| `tests/hub_daemon_lifecycle_test.rs` | include the new module |
| `script/run-loaded-daemon-lifecycle` | `--test-target event-plane-saturation` and a 3600 s run timeout for that target |
| `.github/workflows/loaded-daemon-lifecycle.yml` | one new `test_target` option |
| `examples/event-plane-producer/plugin.lua` | bounded `event_plane.emit_burst` tool; `emit_ready` unchanged |
| `examples/event-plane-producer/botster-package.json` | optional `pad` payload field for the 4 KiB burst body |
| `examples/event-plane-consumer/plugin.lua` | second `sample.ready` handler for the hold seam; original accumulator unchanged |
| `README.md` | pointer to `docs/event-plane-load-proof.md` |

Published npm fixture bytes under `packages/hub-test-support` did not change. No unpublished-version cutover.

## Ownership boundaries preserved

Hub owns the campaign, budget document, generic client-event fixture, and fixture plugins. Production budgets, queue bounds, and scheduling decisions are unchanged. Observability counters and the four `BOTSTER_ENV=test` seams remain the merged work of `ticket_1787267568_492780`. Core, Web, TUI, and Project Pipelines sources were not edited.

## Cross-repo dependencies or separately routed work

All five plan prerequisites are closed. This workflow executed only Hub against its locked Core. Cited, not executed:

| Ticket | Repository | Verify gate |
| --- | --- | --- |
| `ticket_1787278643_145174` | botster-hub | `gate_result_1787298630_529803` |
| `ticket_1787267568_492780` | botster-hub | `gate_result_1787345536_471839` |
| `ticket_1787278658_151737` | botster-project-pipelines | `gate_result_1787350903_317327` |
| `ticket_1787278327_274484` | botster-web | `gate_result_1787368137_788873` |
| `ticket_1787278327_199618` | botster-tui | `gate_result_1787376968_845429` |

## Deviations from plan

1. **Calibration literals are not yet derived.** Section 5A.4 requires a residual-tail dispatch on `ubuntu-24.04` before acceptance. This host is Darwin. The calibration JSON records the immutable workload literals and leaves `thresholds` null. Acceptance must not run until that dispatch commits numbers.
2. **The 300-session campaign is `#[ignore]`.** Default `hub_daemon_lifecycle_test` and `script/run-lifecycle-suite` would otherwise run a 600 s × two-arm campaign. The loaded runner selects `event_plane_saturation_campaign` with `--ignored --exact`.
3. **Measurement-arm MCP and UI use Hub-native `PluginMcpListTools` and `ListApps`.** The decoupled arm admits no package, so product MCP/UI tools would fail and trip 5A.2.1. The cycle stays identical across arms.
4. **`run_client_event_conformance` is Unix-only.** WebRTC event subscribe lives in the campaign helper `subscribe_webrtc_events`, which uses the existing local WebRTC adapter bootstrap fixture, not a product event consumer.

The committed plan's acceptance checks still require the reference-runner calibration and acceptance dispatches. Those remain the load-bearing numeric gates.

## Runtime-teardown lenses

| Lens | Implementation |
| --- | --- |
| Isolation | Quiet sessions and churn sessions are Unix. WebRTC peers use the dedicated runtime. Connection-scoped event holders use `(connection_id, subscription_id)`. |
| Bounds | Existing close, write, and invocation timeouts are unchanged. The campaign run timeout is 3600 s so both 600 s windows can finish. |
| Late-message matrix | `run_late_event_holder_matrix` reuses a subscription id on a new connection after the first connection drops, then unsubscribes. Existing WebRTC late Spawn/Attach/entity tests stay in the suite. |
| Production-path proof | IsolatedHub starts the real `botster-hub` binary. Client event proof uses `connect_for_package_event_subscriptions`, `subscribe_events`, `next_event`, and `take_skipped_events`. |
| Ownership identity | Reused subscription ids are admitted on the replacement connection only. |
| Sibling fail-closed | Unchanged shipped policy: successful close isolates one peer; ultimate close failure sacrifices every peer on the dedicated runtime. The campaign does not assert sibling survival on that path. |

## U4 and U7

- **U4.** Merged `src/event_plane_counters.rs` stores atomics beside the router. `DaemonStatus.observability` reads them without `PackageEventRouter::try_lock`.
- **U7.** `examples/event-plane-producer` already declares a session-scoped `sample.ready` notice. Burst emit uses that event. The consumer does not emit. Malformed-descriptor rejection stays an admission concern of `ticket_1787278643_145174`; it is not a new event-plane runtime fault.

## Tests and downstream proof run

Prebuild:

- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `cargo build --locked --bin botster-hub`

Commands:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 2 passed, 1 ignored
- `./test.sh --locked --test hub_daemon_lifecycle_test isolated_hub_` — 10 passed, including notice-reaction and Unix/WebRTC package-event proofs
- `./test.sh --locked` workspace run (invoked as `./test.sh --locked -p botster-hub-test-support`, which still executes `--workspace`) — exit 0

Live isolated-hub proof recorded `core_sha=7eafa470a18025895995bbedc20d34b58106a03b` and a hub binary under this checkout `target/debug`.

The ignored campaign and the GitHub `event-plane-saturation` dispatch were not run on this Darwin host.

## Unverified behavior or residual risk

- Absolute p50/p95/p99/max/throughput budgets are unpublished until calibration.
- Fleet `N = 300` PTY admission on the reference runner is unknown (plan U1).
- Eleven fault lanes at full fleet size have not executed.
- North Star behavioural oracles under 600 s saturation have not executed.
- Ablation of `events.emit` wait, queue-bound removal, resync drop, and adapter snapshot-phase naming is source-guarded, not a live red-on-revert campaign.

## Missing vault guidance discovered

None that blocked this change. Plan section 13 gaps remain capture candidates: North Star numbers vs oracles, saturation-time counter lock design (now closed by the observability ticket), and `DAEMON_MAX_CONNECTIONS` vs session count.
