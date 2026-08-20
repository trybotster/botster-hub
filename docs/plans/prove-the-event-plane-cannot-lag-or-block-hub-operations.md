# Plan: prove the event plane cannot lag or block Hub operations

Ticket: `ticket_1786663585_879846`
Run: `run_1787262311_549251`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Project: `project_1786663508_823105` Botster Non-Blocking Event Plane, Stage D
Vault checklist: `checklist_1787266824_449406` (ticket scope, one Plan visit)

## 1. Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub`, path `/Users/jasonconigliari/Projects/botster-hub` |
| Run worktree | `project-pipelines/ticket_1786663585_879846` at `b3b54f1f87e29867da4eb371e9b7f3b18160996a` |
| Base | `origin/main` = `b3b54f1f87e29867da4eb371e9b7f3b18160996a` after `git fetch origin --prune`; `git rev-list --count origin/main..HEAD` = 0 |
| Core pin | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Worktree hygiene | tracked `.gitignore` is 53 bytes and equals `git show HEAD:.gitignore`; worktree path has no `:`; no `CARGO_TARGET_DIR` override needed |
| Session-type eligibility consumer | no |
| `teardown_class_applies` | **yes** (see section 11) |

Independent resolution: `project_pipelines_current_context` returns `target_id` `tgt_7e208a0c76a44980a83b63af976b1f22` on both the ticket and the run. `list_spawn_targets` maps that id to `botster-hub`. The process working directory was not used for routing.

## 2. Repository playbook loaded

[[botster-hub-playbook]]

## 3. Other role/surface playbooks and atomic notes loaded

Role and stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-plan-reviewer-playbook]]
- [[botster runtime teardown lenses]]
- [[current botster is a modular repository family not the legacy trybotster monorepo]]
- [[legacy trybotster notes are not current modular botster contracts]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[vault example paths are not repository placement conventions]]

Event-plane contract:

- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[exact owner plus name is the only package event subscription key]]
- [[Package-event subject filters are exact strings compiled at admission]]
- [[admitted event holders survive producer unload until Core completion]]
- [[Client event holders are connection-scoped]]
- [[Client event subscriptions stay on the multiplexed host-control path]]
- [[Fair host-control writing selects already-admitted frames]]
- [[a transient package event cannot be the sole authority for a durable close]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[botster hub event storms must be rejected before queues grow unbounded]]
- [[botster hub event lanes coalesce repeatable work before rejecting under pressure]]
- [[hub event pressure needs bounded flood regressions]]

Owner loop and projection:

- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[Hub background fairness must stay policy-neutral]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]

Measurement discipline (load-bearing for this ticket):

- [[conformance harnesses gate on deterministic invariants not timing]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
- [[load diagnostics must not cost work proportional to what they measure]]
- [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]
- [[verification reports name the load bearing oracle when cheaper suites are blind]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[web event plane budgets are published numeric host limits]]

Provenance and fixtures:

- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub Core pin rolls update eleven literal sites and six lock sources]]
- [[hub test support lacks package event producer fixtures]]
- [[hub client event queue max requires Botster test mode]]

Teardown class support:

- [[file descriptor exhaustion from stale webrtc connections]] (tagged `botster-legacy`; treated as drift evidence, not a current contract)
- [[webrtc peer cleanup removes every per peer owner together]] (tagged `botster-legacy`; same treatment)

## 4. Context loaded

### 4.1 Dependencies

All nine registered dependencies are closed and `blocking_dependencies` is empty:

| Ticket | Repository | Title |
| --- | --- | --- |
| `ticket_1786663581_962361` | botster-core | control-plane lifecycle journal wake and page API |
| `ticket_1786663581_723222` | botster-core | class-aware non-blocking plugin invocation admission |
| `ticket_1786663582_169720` | botster-hub | session state without blocking operation paths |
| `ticket_1786663582_483898` | botster-hub | bounded package event router and exact plugin subscriptions |
| `ticket_1786663583_640263` | botster-hub | client event subscriptions on the host control protocol |
| `ticket_1786663583_568924` | project-pipelines | transient `question.opened` event |
| `ticket_1786663584_427840` | botster-web | consume transient package events |
| `ticket_1786663585_944018` | botster-tui | consume transient package events |
| `ticket_1786661010_115885` | botster-hub | Terminal Transport North Star integration |

No open sibling ticket in `project_1786663508_823105` shares this scope. Every other project ticket is closed.

### 4.2 Existing Hub budget constants (the campaign asserts against these, it does not invent them)

| Constant | Value | Site |
| --- | --- | --- |
| `MAX_OWNER_TURN_MS` | 25 | `src/daemon_maintenance.rs:34` (re-exported `src/lib.rs:130`) |
| `MAX_READY_OPERATION_WAIT_MS` | 50 | `src/daemon_maintenance.rs:36` |
| `OBSERVE_SLICE_BUDGET` | 8 sessions / 64 KiB / 8 ms | `src/daemon_maintenance.rs:39` |
| `BASELINE_PAGE_BUDGET` | 16 rows / 64 KiB / 8 ms | `src/daemon_maintenance.rs:46` |
| `EVENT_DELIVERY_MAX_ITEMS` / `_BYTES` / `_ELAPSED` | 8 / 32 KiB / 8 ms | `src/daemon_maintenance.rs:1156-1158` |
| `SESSION_DELIVERY_MAX_ITEMS` / `_BYTES` / `_ELAPSED` | 16 / 64 KiB / 8 ms | `src/daemon_entity_subscriptions.rs:29-31` |
| `PUMP_MAX_*` | 8 each | `src/daemon_maintenance.rs:66-74` |
| `DAEMON_MAX_CONNECTIONS` | 64 | `src/daemon_transport.rs:156` |
| `DAEMON_MAX_FRAME_BYTES` | 1 MiB | `src/daemon_transport.rs:155` |
| `DAEMON_CONTROL_QUEUE_CAPACITY` | 256 | `src/daemon_transport.rs:158` |
| `MAX_HOST_FRAMES_PER_FLUSH_TURN` | 3 | `src/host_control_fair_write.rs:19` |
| client event subject caps | 16 / 256 B / 4096 B / 64 subs | `src/daemon_event_subscriptions.rs:22-25` |

Router policy is configurable, not constant. `PackageEventPlaneOptions::default()` at `src/config.rs:345-362` supplies payload 64 KiB, 64 subscriptions per plugin, 64 subscribers per event, fanout 64, producer queue 256 events / 512 KiB, consumer queue 128 events / 2 MiB, global in-flight 16 MiB, rate 100 per second with burst 200, and queue age 1000 ms. There is no TOML loader (`src/config.rs:1-6`); production values are code literals in `src/main.rs`.

### 4.3 Existing campaign infrastructure that this plan reuses

| Asset | Purpose |
| --- | --- |
| `script/run-loaded-daemon-lifecycle` | bounded CPU stress profiles, repetition loop, per-run and campaign deadlines, run-token ownership, zombie settle, artifact bundle |
| `.github/workflows/loaded-daemon-lifecycle.yml` | fresh `ubuntu-24.04` VM, `workflow_dispatch`, `test_target` choice list |
| `script/run-lifecycle-suite` | dirty-host gate and the `clean` / `product_failure` / `host_exhaustion` / `environment_tainted` verdict classifier |
| `script/process-census` | cross-platform process, zombie, and dev-artifact census |
| `script/probe-hub-resources` | bounded per-phase JSON probe against a caller-owned Hub |
| `tests/hub_daemon_lifecycle/harness.rs` | `harness_budget_expired` markers, `probe_fd_limit`, `probe_pty_allocation`, owned-session cleanup |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | `IsolatedHubBuilder` with per-child env |
| `examples/event-plane-producer`, `event-plane-consumer`, `event-plane-cycle`, `synthetic-plugin` | checked-in package-event fixtures |
| `docs/hub-resource-proof.md`, `docs/lifecycle-suite-harness.md`, `docs/loaded-daemon-lifecycle-runner.md` | the top-level proof-contract doc tier this campaign joins |
| `docs/reports/bounded-hub-resources-fresh-campaign-evidence.json` | the machine-readable campaign-evidence schema to mirror |

Baseline commands already executed in this worktree at `b3b54f1`:

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `cargo build --locked --bin botster-hub` | exit 0 |
| `./test.sh --locked --test session_projection_owner_loop` | `ok. 5 passed; 0 failed` |
| `./test.sh --locked --test hub_daemon_lifecycle_test isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree -- --exact` | `ok. 1 passed; 0 failed; 264 filtered out` |

The base therefore reaches test execution on both surfaces this ticket touches.

### 4.4 Two blocking findings from repository evidence

**Finding 1 — Hub cannot record seven of the twelve signals the ticket requires.**

The ticket requires the campaign to record queue count and bytes, oldest age, admission latency, delivery latency, shed, gap, resync, pressure, timeout, owner-turn duration, and ready-operation wait. Repository search shows:

| Signal | Current state |
| --- | --- |
| queue count, queue bytes, global in-flight bytes | available on `EventPlaneSnapshot` (`src/package_event_router.rs:163-172`), reachable through `PackageEventRouter::snapshot` (`:800`) |
| resync | available as `lifecycle_resync_reads`, `package_entity_resync_attempts`, `package_entity_resync_degraded` on `DaemonLifecycleCounters` |
| pressure | available Core-side on `PluginWorkerPluginDebugSnapshot` (`*_pressure_events`, `*_saturated`) and Hub-side as `DaemonEgressDiagnostics` write failures |
| **oldest queue age** | **absent.** Age is only a predicate at `src/package_event_router.rs:599` and `src/daemon_event_subscriptions.rs:266`; no value is reported |
| **admission latency** | **absent** |
| **delivery latency** | **absent** |
| **shed count** | **absent.** `EventPlaneStatus` is a per-call return value and is never accumulated |
| **gap count** | **absent.** Client gaps are a per-subscription `AtomicBool` (`ClientGapSlot`, `src/daemon_event_subscriptions.rs:122-130`) |
| **owner-turn duration** | **written but unreachable.** `MaintenanceState.last_owner_turn` (`src/daemon_maintenance.rs:619`) is set at `src/daemon_transport.rs:293`, but `daemon_maintenance` is a private module and the value never enters `DaemonStatus` |
| **ready-operation wait** | **never measured in production.** It exists only as a test assertion |

`grep -rni "latency"` across `src/` and `crates/` returns three comment hits and no measurement.

A second constraint compounds this. `PackageEventRouter::snapshot` takes the router mutex through `try_lock` and returns `ShedBusy` under contention ([[router ingress uses try_lock only and contention is shed_busy]]). Under exactly the saturation this campaign creates, the only existing queue-depth read is the most likely to fail. The observer disappears when it matters most.

This ticket is therefore not test-only. It needs a bounded production observability increment first. Section 6 registers that increment as a dependency instead of broadening this run.

**Finding 2 — the North Star publishes no numeric terminal input or output budget.**

The ticket's acceptance says terminal input and output must "stay within their existing North Star budgets." No such number exists. `docs/plans/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui.md` publishes behavioural oracles (identity, ordering, bytes, late-attach history, resize, input, cancellation, reconnect, exit, connection loss, session types) and the rule that Hub cannot inspect terminal bodies. The only numeric terminal-adjacent limits are the Core-owned write budget, whose threshold lives in Core, and the WebRTC 64 KiB plaintext chunk with a 16 MiB declared delivery cap (`docs/client-protocol.md:662`). `docs/client-protocol.md:1189` states explicitly that the many-PTY session counts "are bounded correctness cases, not performance targets or benchmark claims." `README.md` contains no performance, latency, throughput, or scale claim.

Assumption A1 in section 8 records how this plan resolves that.

## 5. Scope

### A. Publish the budget contract before acceptance

Add `docs/event-plane-load-proof.md` in the top-level operator-documentation tier, beside `docs/hub-resource-proof.md`, `docs/lifecycle-suite-harness.md`, and `docs/loaded-daemon-lifecycle-runner.md`. Repository prior art, not vault example paths, selects this destination. The document publishes, before any acceptance run:

1. The reference machine profile: fresh GitHub-hosted `ubuntu-24.04`, its CPU count, its `ulimit -n`, and its PTY ceiling, all recorded from `metadata.txt`.
2. The fleet target: `N = 300` active lightweight sessions, with the FD and PTY precondition that makes that number admissible, and the rule that a shortfall is a `host_exhaustion` verdict rather than a silent reduction.
3. Every numeric budget, each one named separately so a breach identifies its own limit, following [[web event plane budgets are published numeric host limits]]. Budgets divide into three classes:
   - **Deterministic work bounds** taken verbatim from section 4.2. These are pass or fail.
   - **Queue bounds** from `PackageEventPlanePolicy`. These are pass or fail.
   - **Latency and throughput budgets** published in two forms: an absolute p50, p95, p99, and maximum for the reference profile, and a paired relative bound against the same run's decoupled arm. Both forms are pass or fail. Assumption A2 explains the pairing.
4. The exact formulas: how each percentile is computed, how the paired ratio is computed, and which sample population each covers.
5. The recorded-signal list and the verdict rules, reusing the existing vocabulary `clean`, `product_failure`, `host_exhaustion`, `environment_tainted`, and `survivors_present`.

### B. Add the saturation campaign as one new target on the existing runner

- New module `tests/hub_daemon_lifecycle/event_plane_saturation.rs`, registered in `tests/hub_daemon_lifecycle/mod.rs`.
- New `--test-target event-plane-saturation` case in `script/run-loaded-daemon-lifecycle` beside the existing cases, and a matching `options:` entry in `.github/workflows/loaded-daemon-lifecycle.yml`.
- The campaign runs both arms inside one dispatch so both arms share one machine, one kernel, and one ambient load.

### C. The two arms

| Arm | Composition |
| --- | --- |
| `plane-enabled` | `N` lightweight sessions, `examples/event-plane-producer` emitting at and above its configured rate and queue limits, `examples/event-plane-consumer` subscribed, one Unix and one WebRTC host-control client holding event subscriptions, and one attached noisy PTY carrying terminal input and output |
| `plane-decoupled` | the same `N` sessions and the same attached noisy PTY, with no package admitted, no emitter, no plugin subscription, and no client event subscription |

The ticket permits "an event-disabled **or** projection-decoupled baseline." This plan takes the decoupled option deliberately, because the disabled option would require a new production configuration switch that nothing else needs. Adding one would be speculative configurability.

### D. Fleet fixture

Spawn `N` quiet sessions through public `DaemonRequest::Spawn` with a bounded producer command, following the existing quiet-session shape at `crates/botster-hub-test-support/src/lib.rs:639-660`. Advance lifecycle through public `Drain` and bounded `ListSessions` polling. Do not attach to quiet sessions and do not use a sleep as a success condition. Keep the one attached noisy session for the terminal legs.

`run_many_pty_client_attach_conformance` is not reused directly: it attaches, reads screen, captures a snapshot, sends input, and cleans up per session, which is a correctness proof shape rather than a background-load shape. The new fleet fixture reuses its command shapes and its `Spawn`, `Drain`, `ListSessions` sequence.

### E. Fault-injection matrix

Hooks confirmed by source search. "Reachable" means reachable from a spawned `botster-hub` daemon child, which is the only shape the campaign can use.

| Fault | Mechanism | Reachable today | New seam |
| --- | --- | --- | --- |
| Full router ingress, `ShedFull` | emit above `producer_queue_max_events` 256 and `package_rate_per_sec` 100 from `examples/event-plane-producer`; enforcement at `src/package_event_router.rs:469-510` | yes, at production defaults | no |
| Router contention, `ShedBusy` | `PackageEventRouter::test_with_inner_held` (`src/package_event_router.rs:842`, `#[doc(hidden)]`, not `cfg(test)`) | in-process only | no, focused lane |
| Full consumer mailbox, client side | `BOTSTER_ENV=test` plus `BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX` (`src/daemon_event_subscriptions.rs:551`), driven by `BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH` (`src/daemon_transport.rs:922`) | yes | no |
| Full consumer mailbox, plugin side | slow handler against `consumer_queue_max_events` 128 | yes | no |
| Plugin worker restart | public reload, disable, enable (`src/daemon_package_control.rs:130`, `:383`) | yes | no |
| Client reconnect, Unix | reconnect churn plus `BOTSTER_HUB_UNIX_EOF_ABLATION` (`src/daemon_transport.rs:5736`) | yes | no |
| Client reconnect, WebRTC | `BOTSTER_HUB_TEST_CLOSE_LOCAL_WEBRTC_OPERATION` (`src/local_webrtc.rs:1485`) | yes | no |
| **Dropped lifecycle wake** | none. `take_journal_advanced_wake` (`src/runtime.rs:3184`) is a destructive coalesced bit. Only pure-function seams exist (`src/daemon_maintenance.rs:819`, `:908`) | **no** | **yes** |
| **Lifecycle cursor expiry** | `HubRuntime::with_test_lifecycle_journal_capacity` (`src/runtime.rs:4598`) is `#[cfg(test)]`, so an integration test or live daemon cannot shrink `DEFAULT_LIFECYCLE_JOURNAL_CAPACITY` 1024 | **no** | **yes** |
| **Slow handler beyond the invocation timeout** | `examples/event-plane-consumer/plugin.lua:7-13` burns CPU, but `DEFAULT_INSTRUCTION_BUDGET` 500_000 (`src/lua_runtime.rs:55`) aborts it before 1000 ms. It proves non-blocking dispatch, not latency | partial | **yes** |
| **Handler timeout** | `timeout_ms: 1_000` is a literal at `src/daemon_maintenance.rs:1121` and `:1274`; `PLUGIN_EVENT_TIMEOUT_MS` at `src/runtime.rs:147`. No override exists | **no** | **yes** |

Four seams are therefore missing. Each follows the existing precedent exactly: one `BOTSTER_ENV=test` gated environment read in `core_daemon_config` (`src/runtime.rs:4612-4641`), in the same style as `BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY` at `src/runtime.rs:4618`. Core already supplies fourteen `CoreDaemonConfig::with_test_*` builders; only four are plumbed to Hub environment reads today. The four seams belong to the dependency in section 6, not to this ticket.

The campaign uses production default policy values. It does not shrink `PackageEventPlaneOptions` for the main arms, because shrinking the queue would prove shed behaviour at an artificial bound rather than at the shipped one. Shrunken-policy lanes stay focused unit lanes.

### F. Evidence

- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`, the narrative report.
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`, modelled on `docs/reports/bounded-hub-resources-fresh-campaign-evidence.json`, recording exact revisions for all five repositories, configuration, machine profile, formulas, per-arm results, the fault matrix outcomes, and the verdict.

## 6. Repository ownership boundaries and cross-repo dependencies

### 6.1 Ownership

| Concern | Owner |
| --- | --- |
| Budget publication, campaign harness, verdict rules, evidence | botster-hub (this ticket) |
| Event-plane counters and their read path | botster-hub (dependency in 6.2) |
| Lifecycle journal, wake, pages, plugin admission classes | botster-core, unchanged |
| `question.opened` contract | botster-project-pipelines, unchanged |
| Transient-event consumption | botster-web and botster-tui, unchanged |
| Terminal bytes | Core `SessionIo` and `ClientWorker`, unchanged |

Hub must not gain terminal body access, Workspaces policy, or package product policy through this campaign. The existing architecture tests at `src/unix_terminal_adapter.rs:905`, `src/webrtc_terminal_adapter.rs:915`, and `src/daemon_attach_stream.rs:1133` stay in the gate list.

### 6.2 One same-repository blocking dependency to register

Registered: `ticket_1787267568_492780` against `tgt_7e208a0c76a44980a83b63af976b1f22` (botster-hub), linked as `dependency_1787267572_315049`. Same repository, so this is a sibling-surface dependency rather than a cross-repository one. It follows the project's established split, where `ticket_1787104273_140454` and `ticket_1786733177_803101` each supplied a surface before their consumer implemented against it.

Title: Hub: publish bounded event-plane observability counters and four load-campaign seams.

Contract:

1. Add monotonic, bounded, O(1)-per-event counters for shed by typed reason, client gap, admission attempts, and delivery attempts. Counters must not allocate or scan per event, following [[load diagnostics must not cost work proportional to what they measure]].
2. Add an oldest-queue-age value for each producer queue, each consumer queue, and each client mailbox. Age is already computed as a predicate; report the value.
3. Add bounded admission-latency and delivery-latency observations. A fixed-bucket histogram or a reservoir with a fixed cell count is acceptable. An unbounded sample vector is not.
4. Surface `last_owner_turn` and add a ready-operation wait measurement, then expose both through the existing `DaemonStatus` path rather than a new transport.
5. **The read path must not contend with `PackageEventRouter`'s mutex.** Counters read during saturation must not return `ShedBusy`. Use atomics beside the router, not a `try_lock` snapshot.
6. Add the four `BOTSTER_ENV=test` gated seams from section 5E, each as one environment read in `core_daemon_config` (`src/runtime.rs:4612-4641`) in the same style as `BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY`:
   - drop the next journal-advanced wake a bounded number of times;
   - set the lifecycle journal capacity, promoting the existing `#[cfg(test)]` `with_test_lifecycle_journal_capacity` (`src/runtime.rs:4598`) to an integration-reachable value;
   - set the package-event invocation `timeout_ms` used at `src/daemon_maintenance.rs:1121` and `:1274`;
   - hold a package-event handler for a bounded duration, so a handler can exceed the invocation timeout despite the Lua instruction budget.
7. Do not change any production budget, queue bound, or scheduling decision.

Acceptance for that dependency: every signal in the ticket's recorded-signal list is readable through a public daemon request; a saturation unit lane proves the read path still returns values while the router sheds; and the existing owner-turn and ready-operation invariants stay unchanged.

Consumer note for that ticket: this ticket `ticket_1786663585_879846` depends on that surface. Do not implement the campaign there.

## 7. Non-scope

- No change to `MAX_OWNER_TURN_MS`, `MAX_READY_OPERATION_WAIT_MS`, `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, `PUMP_MAX_*`, `EVENT_DELIVERY_*`, `SESSION_DELIVERY_*`, or any `PackageEventPlaneOptions` default.
- No production event-disable switch.
- No new transport, request vocabulary change, `PROTOCOL_VERSION` bump, or conformance-fixture revision bump.
- No source change in botster-core, botster-web, botster-tui, botster-tui-kit, or botster-project-pipelines. Their revisions are recorded, not modified.
- No `--test-threads=1`, no `serial_test`, and no nextest. Repository policy forbids serialization as acceptance evidence (`docs/plans/isolate-lifecycle-suite-workers-and-host-resources.md:154`).
- No retry loop that discards a red repetition.
- No change to `run_many_pty_client_attach_conformance` or its published session counts.

## 8. Assumptions and unknowns

### Assumptions

**A1 — "existing North Star budgets" do not exist as numbers.** Section 4.4 Finding 2 establishes that the Terminal Transport North Star publishes behavioural oracles, not numeric input or output budgets, and that `docs/client-protocol.md:1189` explicitly disclaims performance targets. This plan therefore reads that acceptance line in two parts and satisfies both:

1. The campaign proves the North Star **behavioural** oracles still hold on the attached noisy session while the event plane is saturated: identity, ordering, exact bytes including non-UTF-8, late-attach history, resize, input, cancellation, reconnect, and `ProcessExited`.
2. The campaign **publishes new numeric terminal input and output budgets** in `docs/event-plane-load-proof.md`, labelled as newly published by this campaign rather than pre-existing. Nothing is waived; a missing budget is created rather than skipped.

**A2 — latency budgets are published in absolute and paired-relative form, and both gate.** [[conformance harnesses gate on deterministic invariants not timing]] says wall-clock durations should be observations rather than pass-or-fail assertions. [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] and [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]] say the same for owner-turn and ready-operation claims. The ticket nevertheless requires published latency budgets as acceptance. This plan resolves the tension without weakening either side:

- **Scheduler and boundedness claims gate on deterministic oracles only.** Owner-turn and ready-operation precedence use decision-level and work-bound oracles, never elapsed time. Queue bounds gate on counts and bytes.
- **Operation latency gates on a paired ratio.** Both arms run in one dispatch on one machine, so ambient load is common to both. The gate is `p99(plane-enabled) <= p99(plane-decoupled) * R + S` for each operation, with `R` and `S` published before the run.
- **Absolute p50, p95, p99, and maximum are also published and also gate, but only on the declared reference profile.** On any other machine they are recorded as observations and the report says so.

This keeps every published budget as acceptance while keeping the architectural claim on oracles that do not depend on runner speed.

**A3 — the reference runner is the existing loaded workflow.** `docs/loaded-daemon-lifecycle-runner.md` establishes a fresh GitHub-hosted `ubuntu-24.04` VM as the isolated campaign home. `script/run-loaded-daemon-lifecycle:141-144` forces `--stress-profile none` on Darwin, and `script/run-lifecycle-suite` refuses a dirty host. A developer machine that hosts a live Botster hub cannot produce this evidence. The declared machine profile is therefore the workflow runner.

**A4 — Web and TUI participate as pinned consumer checkouts, not as source changes.** `script/prove-north-star-shared-session` already establishes that Hub owns a cross-client coordinator driving `BOTSTER_WEB_CHECKOUT` and `BOTSTER_TUI_CHECKOUT`. The campaign records all five repository revisions and drives the Web and TUI legs through that established pattern. No file changes in those repositories. If a change turns out to be required, register a dependency against that repository's target rather than editing it from this run.

**A5 — the baseline arm is projection-decoupled, not event-disabled.** The ticket permits either. Section 5C explains why the decoupled arm is chosen.

**A6 — `N = 300`.** "Hundreds" is satisfied at 300. The existing ceiling is 32, in an `#[ignore]` local test (`tests/hub_daemon_lifecycle/sessions.rs:5670`). 300 is a tenfold increase over any exercised count.

### Unknowns for Plan Review and Implement to resolve

**U1 — can the runner sustain 300 concurrent PTY sessions?** Each session consumes file descriptors and a PTY pair. `probe_fd_limit` and `probe_pty_allocation` (`tests/hub_daemon_lifecycle/harness.rs:244`, `:258`) already exist to classify exhaustion. Implement must measure the runner's `ulimit -n` and PTY ceiling first and record them in the published profile. If 300 is not admissible, the outcome is a `host_exhaustion` verdict and an escalation, not a silent reduction to a number that fits.

**U2 — `DAEMON_MAX_CONNECTIONS` is 64.** Sessions are not connections, so 300 sessions are admissible. The campaign must keep its client count well below 64 and must not confuse the two limits. The existing 64-connection saturation case (`tests/hub_daemon_lifecycle/sessions.rs:3324`) stays a separate concern.

**U3 — does natural saturation reach lifecycle cursor expiry?** `DEFAULT_LIFECYCLE_JOURNAL_CAPACITY` is 1024. Whether 300 churning sessions can outrun the Hub cursor by more than 1024 changes is unmeasured. The dependency seam removes the uncertainty; Implement should still record whether natural expiry occurred.

**U4 — the reported `EventPlaneSnapshot` read path.** Section 6.2 item 5 requires a read path that does not contend with the router mutex. Implement must confirm the dependency delivered that property before trusting any saturation-time queue reading.

**U5 — an existing budget test is nominal.** `published_owner_turn_budgets_fail_if_observe_walks_every_session` (`tests/session_projection_owner_loop.rs:207`) contains only `const` assertions and one discarded comparison. Despite its name it cannot fail if observe walks every session. The campaign must not cite it as owner-turn evidence. Whether to repair or rename it is a separate decision recorded as a vault gap in section 13.

## 9. Affected surfaces and files

### New

| Path | Purpose |
| --- | --- |
| `docs/event-plane-load-proof.md` | published budget contract, machine profile, formulas, verdict rules |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | the campaign lanes |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | narrative report |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | machine-readable campaign evidence |

### Modified

| Path | Change |
| --- | --- |
| `tests/hub_daemon_lifecycle/mod.rs` | register the new module |
| `script/run-loaded-daemon-lifecycle` | one new `--test-target event-plane-saturation` case beside the existing map at `:978-1024` |
| `.github/workflows/loaded-daemon-lifecycle.yml` | one new `options:` entry in the `test_target` choice list at `:14-30` |
| `examples/event-plane-producer/plugin.lua` | add a bounded burst emitter for saturation; keep the existing single-emit tool unchanged |
| `examples/event-plane-consumer/plugin.lua` | add a slow-path handler behind the new bounded hold seam; keep the existing handler unchanged |
| `README.md` | one pointer to `docs/event-plane-load-proof.md`, matching how `README.md:430` points to `docs/hub-resource-proof.md` |

### Explicitly untouched

`src/daemon_maintenance.rs`, `src/daemon_transport.rs`, `src/daemon_entity_subscriptions.rs`, `src/package_event_router.rs`, `src/package_event_schema.rs`, `src/package_entity_fanout.rs`, `src/config.rs`, `src/host_control_fair_write.rs`, `src/session_projection.rs`, `src/unix_terminal_adapter.rs`, `src/webrtc_terminal_adapter.rs`, `src/daemon_attach_stream.rs`, `Cargo.toml`, `Cargo.lock`, and every Core-family pin literal. Production observability changes belong to the dependency in section 6.2.

## 10. Risks

| # | Risk | Mitigation |
| --- | --- | --- |
| R1 | The campaign becomes a flaky wall-clock gate and later agents delete it | A2 puts the architectural claim on deterministic oracles and the latency claim on a paired within-run ratio. The report names the load-bearing oracle per claim, following [[verification reports name the load bearing oracle when cheaper suites are blind]] |
| R2 | Instrumentation creates the pressure it measures | [[load diagnostics must not cost work proportional to what they measure]] is a hard constraint on the dependency. Counters are O(1) per event, allocate nothing per event, and hoist invariant work out of measured loops |
| R3 | The queue-depth read fails exactly under saturation | Section 6.2 item 5 forbids a `try_lock` read path for saturation-time counters |
| R4 | Host exhaustion is reported as a product failure | Reuse the existing `harness_budget_expired test=<name>` per-test markers and the `run-lifecycle-suite` verdict ordering. Global marker presence is not enough; each failed test needs its own marker |
| R5 | A red repetition is hidden by a retry | The runner already stops at the first red repetition. The plan forbids retry loops that discard a red result |
| R6 | 300 sessions leak processes, PTYs, or descriptors | The runner already carries run-token ownership, session and process-group ledgers, TERM to KILL escalation, and settled zombie deltas. Survivors fail the repetition |
| R7 | A wrong `N` silently weakens the claim | U1 makes a shortfall a `host_exhaustion` verdict plus escalation, never a quiet reduction |
| R8 | Legacy vault notes are applied as current contracts | The two `botster-legacy` WebRTC notes are treated as drift evidence. Current behaviour is verified in `src/local_webrtc.rs` per [[legacy trybotster notes are not current modular botster contracts]] |
| R9 | The campaign claims readiness the evidence does not support | The ticket's own rule applies: do not claim hundreds-of-sessions readiness unless this campaign passes. The report states the exact `N`, profile, and verdict |
| R10 | Scope creep into production tuning | Section 7 forbids any change to a budget, queue bound, or scheduling decision. A breach is a finding, not a tuning opportunity |

## 11. Runtime-teardown class

`teardown_class_applies`: **yes.** The campaign drives session and `ClientWorker` teardown at 300 sessions, plugin worker restart, Unix and WebRTC client reconnect, and file-descriptor and PTY pressure. It also compares terminal state against live runtime state. [[botster runtime teardown lenses]] applies. Every required field is answered below.

**`teardown_isolation`.** One failed peer or session must kill only its own ownership set. For a WebRTC peer that set is the channel, send task, transport runner, ping task, recovery state, offer state, ICE state, and connection timing state. For a client event holder the set is `(connection_id, subscription_id)` only ([[Client event holders are connection-scoped]]); connection cleanup must not remove a same-named holder on another connection. For a session the set is its terminal subscriptions and attach routes. Healthy siblings must keep working. The campaign asserts sibling survival explicitly after each injected fault, at 300 sessions, so a shared-resource sacrifice cannot hide inside the fleet.

**`teardown_bounds`.** No unbounded wait on the control plane. Existing bounds stay authoritative and unchanged: WebRTC close 3 s in production and 200 ms in test with a 2 s handler join (`src/local_webrtc.rs:60`, `:63`, `:67`); `DAEMON_CLIENT_WRITE_TIMEOUT`, `DAEMON_HANDSHAKE_TIMEOUT`, and `DAEMON_INCOMPLETE_FRAME_TIMEOUT` at 2 s each (`src/daemon_transport.rs:152-154`); package-event invocation timeout 1000 ms. The named hard stop when a library close path misbehaves is the existing `BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD` subprocess oracle (`src/local_webrtc.rs:7175`), which makes an ablated close timeout finite-red. The campaign runs a close-hang lane so a hang under load is a red repetition rather than a stall.

**`late_message_matrix`.** Every ownership-creating surface reachable under saturation:

| Message | Owner tag | Rejection after terminal failure | Residual sweep racing close |
| --- | --- | --- | --- |
| `Attach` | `(session_id, subscription_id, generation)`, Core-owned | pre-`READY` failure creates no attach ownership | route-aware idempotent cleanup; occupancy uses the live attach route set |
| `SubscribeEntities` | connection id plus subscription id | connection cleanup drops only that connection's rows | reconciliation sweep; `subscriber_overflow` forces resync, never silent loss |
| `SubscribeEvents` | `(connection_id, subscription_id)`; Unix `client_id`, WebRTC `grant_id` | `UnsubscribeEvents` applies to the calling connection only | connection cleanup drops only that connection's holders |
| `UnsubscribeEvents` | same | same | same |
| Peer-originated request over WebRTC | `grant_id` | closed grant rejects the request | `PeerClosed` removes the whole per-peer owner set together |
| Admitted event holder | producer plus Core job id | producer unload removes contracts and queued copies | an `AdmittedHolder` survives until `CompletionDrain`; late completion is idempotent and cannot drive bytes below zero |

The campaign drives each row under saturation and after each injected fault. A plan that guarded only subscriptions would be incomplete; attach and peer-originated requests are included.

**`production_path_proof`.** The exact production path is: terminal or peer signal, then the production handler, then forget or remove, then idle. The campaign proves the live path, not a terminal JSON record. [[terminal webrtc failure records do not prove peer runtime teardown]] rules out a persisted failure record as teardown evidence. The oracles are the existing `DaemonLifecycleCounters` returning to their baseline, the Hub thread census staying at or below 64, `script/probe-hub-resources` converging, and the runner's owned-session, owned-token, and settled-zombie censuses being empty. The idle-CPU observation uses the existing `BOTSTER_ASSERT_IDLE_CPU_BOUND` gate rather than a new one.

**`ownership_identity`.** Every peer-created durable row carries a stable owner id, listed in the matrix above. A delayed `PeerClosed` snapshot must not delete a row now owned by a different live peer that reused a subscription id. The campaign runs reconnect churn with deliberately reused subscription ids and asserts both queue orders: closed-first and message-first.

**`sibling_fail_closed_policy`.** On successful close, siblings keep working; the campaign asserts that at 300 sessions after every fault. On ultimate close failure the blast radius is bounded to the failing peer or session and its own ownership set; no sibling is sacrificed. A sibling loss is a product failure, never accepted residual risk.

## 12. Acceptance checks and tests

### 12.1 Preconditions, in order

1. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` ([[Hub suite runs prebuild the session worker before the locked test wrapper]]).
2. `cargo build --locked --bin botster-hub`.
3. Record the Hub SHA and the separately locked Core SHA from `Cargo.lock`, then resolve both executable realpaths under the candidate checkout ([[live hub proof records distinct hub and locked core binary provenance]]).
4. Precompile the exact campaign test target before any synthetic load starts ([[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]).

### 12.2 Repository gates

| Command | Requirement |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked` | one `test result:` tally, zero failures |
| `script/run-lifecycle-suite` | `verdict=clean` |

### 12.3 Deterministic gates, the architectural claim

These do not depend on runner speed and are the load-bearing oracles for the ticket's central assertion.

1. **No operation waits on the event plane.** Prove that `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, `Shutdown`, MCP, UI, and entity handlers perform no schema validation, routing, handler invocation, fanout, queue-space wait, or client write. The existing source-level guard style at `tests/session_projection_owner_loop.rs:169` extends to the operation handlers.
2. **`events.emit` never waits.** One non-blocking `try_ingress` attempt on the plugin worker thread; contention returns `shed_busy` with no retry.
3. **Every queue is bounded by count and bytes.** Assert observed maxima against `producer_queue_max_events` 256, `producer_queue_max_bytes` 512 KiB, `consumer_queue_max_events` 128, `consumer_queue_max_bytes` 2 MiB, and `global_in_flight_bytes` 16 MiB, at the shipped defaults.
4. **Owner-turn work bounds hold.** Assert item and byte work per slice against `OBSERVE_SLICE_BUDGET`, `BASELINE_PAGE_BUDGET`, `EVENT_DELIVERY_MAX_ITEMS` and `_BYTES`, `SESSION_DELIVERY_MAX_ITEMS` and `_BYTES`, and `PUMP_MAX_*`. Use `Duration::MAX` for paging and assert work, never elapsed time, following [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]].
5. **Ready-operation precedence holds.** A decision-level test proves `try_recv` runs before a due maintenance slice, and that one turn never stacks maintenance plus a second observe or pump ahead of the next queued-control check.
6. **Session state converges through gap plus complete baseline resync.** After a forced gap, durable state is restored from a complete baseline. An incomplete page is not usable as finished evidence.
7. **Transient events shed when stale.** Events older than `queue_age` 1000 ms shed instead of arriving stale.
8. **Received notices never exceed emitted events.** A gap can reduce notices; it can never create them.
9. **Hub cannot inspect terminal bodies.** The three existing architecture tests at `src/unix_terminal_adapter.rs:905`, `src/webrtc_terminal_adapter.rs:915`, and `src/daemon_attach_stream.rs:1133` stay green, and the campaign additionally asserts that event traffic causes zero terminal adapter API calls and zero terminal queue growth.
10. **North Star behavioural oracles hold under saturation** on the attached noisy session: identity, ordering, exact non-UTF-8 bytes, late-attach history, resize, input, cancellation, reconnect, and `ProcessExited`.
11. **Teardown matrix.** Every row of section 11's late-message matrix, in both closed-first and message-first orders, with reused subscription ids.
12. **Sibling survival** after each of the injected faults, at full fleet size.

### 12.4 Published budget gates

For each of `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, `Shutdown`, MCP, UI, entity, terminal input, and terminal output, in both arms:

- record p50, p95, p99, maximum, and throughput;
- gate on the paired ratio from A2;
- gate on the published absolute budget when the run is on the reference profile;
- record queue count and bytes, oldest age, admission latency, delivery latency, shed by typed reason, gap count, resync count, pressure, timeout, owner-turn duration, and ready-operation wait.

The last item requires the section 6.2 dependency. Without it these signals cannot be recorded and the ticket cannot pass.

### 12.5 Fault lanes

Each of the eleven faults in section 5E runs at full fleet size. Each asserts: the typed result is the expected one; no queue exceeds its bound; siblings survive; durable state converges; and no operation blocks.

### 12.6 Ablation, the red-on-revert requirement

[[a regression test must be shown to go red with the fix reverted]] requires per-claim negative controls, because an early failure can stop later assertions. The campaign records an ablation truth table for at least:

- the non-blocking ingress claim, ablated by making `events.emit` wait;
- the queue-bound claim, ablated by removing one bound;
- the gap-plus-baseline convergence claim, ablated by dropping the resync;
- the terminal content-blindness claim, ablated by naming a snapshot phase in an adapter.

The report names, per claim, which tier detected the defect and which tier was blind, following [[verification reports name the load bearing oracle when cheaper suites are blind]].

### 12.7 Campaign dispatch

```sh
gh workflow run loaded-daemon-lifecycle.yml \
  --ref main \
  -f subject_sha=<exact merged Hub SHA> \
  -f test_target=event-plane-saturation \
  -F repetitions=<published> \
  -f stress_profile=residual-tail
```

The runner stops at the first red repetition. Preserve that artifact before any further dispatch. Green repetitions at a bounded budget mean "not reproduced under that budget", never "resolved".

## 13. Vault gaps worth capturing

1. **The Terminal Transport North Star publishes oracles, not numeric budgets.** Downstream tickets already assume "existing North Star budgets" exist. Capture the correction and name this campaign as the first publisher of terminal numbers.
2. **The Hub event plane has no latency, age, shed-count, or gap-count observability.** Capture the exact absence list from section 4.4 so no later plan assumes these are readable.
3. **`EventPlaneSnapshot` is unreadable exactly under saturation** because `PackageEventRouter::snapshot` uses `try_lock`. Capture this as a diagnostic-design rule: a saturation-time counter must not compete for the lock whose contention it reports.
4. **`published_owner_turn_budgets_fail_if_observe_walks_every_session` is nominal.** Capture that a test name can assert a claim its body cannot fail on, and that budget claims need a work-bound body.
5. **Only four of Core's fourteen `with_test_*` builders are plumbed to Hub environment reads.** Capture the inventory so future fault lanes reach for the existing builder before inventing a seam.
6. **`DAEMON_MAX_CONNECTIONS` 64 is a client bound, not a session bound.** Capture the distinction, because "hundreds of sessions" invites the confusion.

## 14. Pipeline gates and artifacts

| Item | Value |
| --- | --- |
| Gate | `botster_stack_plan_gate` |
| Plan artifact | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md`, attached with `project_pipelines_add_artifact` |
| Vault checklist | `checklist_1787266824_449406` |
| Registered dependency | `ticket_1787267568_492780` (botster-hub), `dependency_1787267572_315049`, status open |
| Blocking effect | this ticket cannot start Implement until `ticket_1787267568_492780` closes |
| Delivery | direct merge into `main`; no pull request; no human pull-request sign-off |
