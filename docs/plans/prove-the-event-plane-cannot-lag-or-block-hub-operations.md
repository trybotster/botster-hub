# Plan: prove the event plane cannot lag or block Hub operations

Ticket: `ticket_1786663585_879846`
Run: `run_1787262311_549251`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Project: `project_1786663508_823105` Botster Non-Blocking Event Plane, Stage D
Vault checklist: `checklist_1787266824_449406` (ticket scope, one Plan visit)

## Plan revision 3

Revision 3 answers Plan Review `review_1787270029_776949` (`changes_required`, four findings). All four were correct.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787270029_463152` Downstream Web and TUI proof is not wired to the claimed production path | **blocker / product** | Verified both repositories directly. botster-web `71b461c2` cannot run the proof: the package-events lane exits at `:275` before the shared lane starts at `:299`, the producer is installed only on the isolated-hub path (`:8783-8785`), identity is hardcoded to `web-prod` (`:1620`, `:1625`, and `fixtures/package-events/plugin.lua:11`), and the producer is a local fixture surface action, with `project_pipelines_ask_human` appearing zero times in the repository. botster-tui `0032fe97` maps `ghostty-shared` to a terminal-only test with no package-event assertion, while its package-event proof runs on its own isolated Hub. Section 5G.1 records the evidence, revision 2's no-source-change claim is withdrawn in A4, and two dependencies are registered against their own targets: `ticket_1787270342_754581` (botster-web) and `ticket_1787270386_991884` (botster-tui). |
| `finding_1787270029_181179` The plan omits required p50, p95, and throughput budgets | high / product | Section 5A.3 now defines calibration formulas and acceptance gates for all five metrics, not two. p50, p95, and p99 gate as ceilings at `1.20` absolute and `R + S` relative; maximum gates at `3.00`; throughput gates as a floor with literal retention ratio `T` = 0.80, which is exactly `1 / R`. Section 12.4 states that none of the five is recorded-only. |
| `finding_1787270029_540633` T4 is not a separate timeout counter | high / product | The contradiction was real. T4 is now a genuine counter. The write deadline is distinguishable at its source: `src/daemon_transport.rs:910-913` returns `ErrorKind::TimedOut`, and the error is in scope at both failure sites (`:774`, `:1894`); only the internal `ControlMessage::EgressWriteFailed` drops it. The dependency now classifies the error, carries it on that internal message, and counts timeouts separately in `record_egress_write_failure` (`:3072`), keeping `stalled_writes` as the unchanged all-failure total. No protocol or fixture bump. |
| `finding_1787270029_336032` The fault-lane sibling rule still contradicts shipped WebRTC failure behavior | high / product | My own inconsistency: section 12.3 exempted the fail-closed path while section 12.5 still demanded survival for all eleven faults. Section 12.5 now carries a three-row table matched to the section 5E matrix: ten non-fatal rows assert survival, WebRTC reconnect with a successful close asserts survival, and the same row driven to ultimate close failure asserts the bounded sacrifice plus the unaffected transport boundary. |

## Plan revision 2

Revision 2 answers Plan Review `review_1787268374_271226` (`changes_required`) and two human answers.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787268374_662300` Runtime sibling policy contradicts the production fail-closed path | **blocker / product** | Revision 1 claimed no sibling is sacrificed. That was wrong. `fail_closed_drop_dedicated_runtime` (`src/local_webrtc.rs:333`) takes every live peer and every stale close peer, sweeps their ownership, and drops the dedicated runtime; the test at `src/local_webrtc.rs:7117-7147` requires exactly that, including the message "timeout fail-closed must sacrifice sibling peers". Section 11.6 now states the real bounded sibling-sacrifice policy, its exact blast radius, and why the tradeoff exists, and forbids the campaign from asserting sibling survival on the ultimate-failure path. |
| `finding_1787268374_124555` The fresh-runner plan does not wire Project Pipelines, Web, or TUI proof | high / product | Section 5G now specifies exact checkout and pin inputs for Project Pipelines, Web, and TUI, wires them into the workflow and the campaign, and drives the real `question.opened` producer with the shipped Web and TUI consumers on one live session identity, per [[cross-client acceptance uses one live session identity]]. Section 12 names an authoritative production oracle for each acceptance condition, per [[each acceptance condition names its authoritative production oracle]]. |
| `finding_1787268374_321530` The dependency contract omits the required timeout signal | high / product | Section 4.4 and the section 6.2 dependency contract now define the timeout signal precisely, name its authoritative source, and add its bounded public observation path and test. |
| `finding_1787268374_932684` Numeric acceptance budgets remain undefined | high / product | Escalated as `question_1787268530_910910`. The human chose A plus E with an ownership limit. Section 5A now fixes the machine profile, workload, session count, warm-up, sample count, percentile method, derivation formulas, literal `R` = 1.25, literal `S` = 8 ms, rounding, outlier policy, invalid-run rules, and failure rules before calibration, and splits calibration from acceptance so acceptance samples can never derive a threshold. |
| `finding_1787268374_454069` Runtime teardown coverage is not exact | high / product | Section 11.3 replaces the generic peer-request row with explicit rows for `Spawn`, `Attach`, `SubscribeEntities`, `UnsubscribeEntities`, `SubscribeEvents`, `UnsubscribeEvents`, and admitted event holders, each citing its existing production test. Section 11.4 names the exact chain `LocalWebrtcPeerClosed` to `handle_control_message` to `remove_peer`, and replaces the thread ceiling with owner-specific idle oracles including `dedicated_runtime_worker_threads()`. |
| `finding_1787268374_805714` Required context is not recorded | low / process | Section 3 now records [[botster-architecture]], [[project-pipelines-playbook]], its cross-repository delivery notes, and [[terminal webrtc failure records do not prove peer runtime teardown]]. The vault checklist is updated to match. |

Human answers folded into this revision:

- `question_1787267931_572353` — the review-only dependency exception in section 0.
- `question_1787268530_910910` — terminal budgets are published as event-plane coexistence regression budgets, never as pre-existing North Star budgets or transport service levels, and thresholds follow the two-phase calibration rule in section 5A.

## 0. Routing exception, read this before reviewing

Human answer `question_1787267931_572353` on `2026-08-20` grants a **review-only** dependency exception for this run.

The engine blocks every advance while a dependency ticket is open, including the advance to Plan Review, and `override_unmet_gates` does not cover ticket dependencies. To let Plan Review validate the split before anyone implements it, `dependency_1787267572_315049` was removed **only** to route this run into Plan Review.

The answer's exact conditions:

1. Plan Review must validate the surface-versus-consumer split, the scope of `ticket_1787267568_492780`, all twelve required signals, and the four fault seams.
2. **This run must not advance to Implement while the dependency is absent.**
3. If Plan Review requires changes, keep `ticket_1787267568_492780` unstarted and revise this plan until Plan Review approves.
4. **After approval, re-add `ticket_1787267568_492780` as a dependency of `ticket_1786663585_879846` before any Implement advance.** Use `project_pipelines_add_ticket_dependency`.
5. Then run and merge `ticket_1787267568_492780` first. This integration run stays parked until that dependency closes.

The dependency's absence is a routing mechanism, not a scope decision. Section 6.2 remains the authoritative contract for the dependency.

**Revision 2 status.** Plan Review `review_1787268374_271226` returned `changes_required`. Under condition 3 above, `ticket_1787267568_492780` stays open and unstarted and the dependency edge stays absent while this plan is revised. Condition 4 still applies the moment Plan Review approves.

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
- [[botster-architecture]]
- [[project-pipelines-playbook]]
- [[current botster is a modular repository family not the legacy trybotster monorepo]]
- [[legacy trybotster notes are not current modular botster contracts]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[vault example paths are not repository placement conventions]]

`botster-architecture` is the required Botster map for both role overlays. `project-pipelines-playbook` is required because this run changes dependency workflow policy through the review-only exception in section 0, and because its cross-repository delivery notes govern the downstream proof in section 5G.

Cross-repository acceptance, from [[project-pipelines-playbook]]:

- [[authentic integration starts with the first cross-repository delivery wave]]
- [[cross-client acceptance uses one live session identity]]
- [[each acceptance condition names its authoritative production oracle]]

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

- [[terminal webrtc failure records do not prove peer runtime teardown]] -- current; the reason section 11.4 asserts live peer and runtime oracles instead of a terminal record
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
| **timeout** | **absent, and worse than absent.** See 4.4.1 |

`grep -rni "latency"` across `src/` and `crates/` returns three comment hits and no measurement.

#### 4.4.1 The timeout signal, defined

Plan Review `finding_1787268374_321530` is correct that revision 1 omitted timeout. The signal is real and fully typed inside Core, and completely invisible outside it. It has four distinct sources on this plane, and they must not be merged:

| Source | Where it happens | Observable today |
| --- | --- | --- |
| **T1 package-event handler invocation timeout** | Core `PluginInvocationFailureKind::TimedOut` (`contract/actor.rs:1233`), produced by the deadline waiter (`engine/plugin_worker.rs:2331-2421`) against the 1000 ms `timeout_ms` Hub sets at `src/daemon_maintenance.rs:1271` | **no, and the outcome is discarded.** `run_completion_drain_slice` (`src/daemon_maintenance.rs:1322-1334`) destructures `completion.result` only to read `request_id`. The `Completed` versus `Failed` discriminant is never inspected on the event path and `failure.kind` is never read. A timed-out handler retires identically to a successful one, with no counter and no log |
| **T2 router queue-age expiry** | `src/package_event_router.rs:595-623`; an envelope older than `queue_age` is retired and skipped | **no.** Silently dropped before any handler sees it. `EventPlaneSnapshot` has no expiry field |
| **T3 client-mailbox queue-age expiry** | `src/daemon_event_subscriptions.rs:266-270` | **partially.** It sets a gap bit for that subscription. There is no count, and the gap bit does not distinguish age expiry from queue overflow |
| **T4 transport write timeout** | `DAEMON_CLIENT_WRITE_TIMEOUT` 2 s (`src/daemon_transport.rs:152`), enforced at `:910` and `:2034` | **conflated today, but separable.** The failure path bumps `stalled_writes` (`record_egress_write_failure`, `src/daemon_transport.rs:3072`), which counts every write failure without inspecting the error. The timeout is nonetheless distinguishable at its source: `src/daemon_transport.rs:910-913` returns `std::io::ErrorKind::TimedOut` with the message `daemon client write deadline elapsed`, and the error value is in scope at both failure sites (`:774` and `:1894`). Only `ControlMessage::EgressWriteFailed` drops it, because that message carries `delivery_kind` alone |

Two structural facts make this worse than a missing counter. Hub never references `PluginWorkerEvent` anywhere, so Core's one typed timeout event, `InvocationTimedOut`, is unobservable by construction. And only 6 of the 25 plugin-worker debug-snapshot fields cross the public boundary (`src/client_api.rs:606-613` into `src/daemon_transport.rs:8010-8017`), so no existing public field could carry a timeout even if one were counted.

T1 is also a latent correctness gap, not only an observability gap: today a package-event handler that exceeds its deadline is indistinguishable from one that succeeded.

A second constraint compounds this. `PackageEventRouter::snapshot` takes the router mutex through `try_lock` and returns `ShedBusy` under contention ([[router ingress uses try_lock only and contention is shed_busy]]). Under exactly the saturation this campaign creates, the only existing queue-depth read is the most likely to fail. The observer disappears when it matters most.

This ticket is therefore not test-only. It needs a bounded production observability increment first. Section 6 registers that increment as a dependency instead of broadening this run.

**Finding 2 — the North Star publishes no numeric terminal input or output budget.**

The ticket's acceptance says terminal input and output must "stay within their existing North Star budgets." No such number exists. `docs/plans/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui.md` publishes behavioural oracles (identity, ordering, bytes, late-attach history, resize, input, cancellation, reconnect, exit, connection loss, session types) and the rule that Hub cannot inspect terminal bodies. The only numeric terminal-adjacent limits are the Core-owned write budget, whose threshold lives in Core, and the WebRTC 64 KiB plaintext chunk with a 16 MiB declared delivery cap (`docs/client-protocol.md:662`). `docs/client-protocol.md:1189` states explicitly that the many-PTY session counts "are bounded correctness cases, not performance targets or benchmark claims." `README.md` contains no performance, latency, throughput, or scale claim.

Assumption A1 in section 8 records how this plan resolves that.

## 5. Scope

### A. Publish the budget contract before acceptance

Human answer `question_1787268530_910910` fixes both the nature of the budgets and how their numbers are derived. Add `docs/event-plane-load-proof.md` in the top-level operator-documentation tier, beside `docs/hub-resource-proof.md`, `docs/lifecycle-suite-harness.md`, and `docs/loaded-daemon-lifecycle-runner.md`. Repository prior art, not vault example paths, selects this destination.

#### A.1 What the numbers are, and what they are not

The campaign publishes **event-plane coexistence regression budgets**. They state how much an operation may degrade when the event plane runs beside it.

They are **not** pre-existing North Star budgets. They are **not** general terminal transport service levels. The North Star behavioural contract stays exactly as it is: the campaign proves every one of its oracles under saturation, without changing terminal byte ownership or any terminal path. The final report must keep the two apart in its own words, and must never present a coexistence budget as a transport guarantee.

#### A.2 Fixed before calibration, immutable afterwards

Every parameter below is fixed by this reviewed plan. Neither calibration nor acceptance may change any of them.

| Parameter | Fixed value |
| --- | --- |
| Machine profile | fresh GitHub-hosted `ubuntu-24.04` runner from `.github/workflows/loaded-daemon-lifecycle.yml`. Recorded fields: runner image, architecture, CPU count, total memory, kernel release, `ulimit -n`, PTY ceiling, Rust 1.97.0, Zig 0.16.0 |
| Stress profile | `residual-tail`, identical in both phases |
| Workload | `N` quiet sessions, one attached noisy PTY, one `event-plane-producer` emitting, one `event-plane-consumer` subscribed, one Unix and one WebRTC host-control client each holding an event subscription |
| Session count `N` | 300, identical in both phases |
| Warm-up | discard the first 30 seconds of steady state **and** the first 20 samples of each operation, whichever ends later |
| Minimum sample count | 200 post-warm-up samples per operation per arm |
| Percentile method | nearest-rank on the ascending sample vector, no interpolation. For `n` samples, `p` is the sample at index `ceil(p * n)`, 1-based |
| Literal `R` | **1.25** |
| Literal `S` | **8 ms** |
| Rounding | every derived millisecond threshold rounds **up** to the next whole millisecond; ratios compute in `f64` and compare at three decimal places |
| Outlier policy | **none.** No sample is discarded after warm-up. Maximum is reported and gated in its own right |

`S = 8 ms` is not arbitrary. It is exactly `EVENT_DELIVERY_MAX_ELAPSED` (`src/daemon_maintenance.rs:1158`), which equals `OBSERVE_SLICE_BUDGET.max_elapsed` and `BASELINE_PAGE_BUDGET.max_elapsed`. The slack therefore says: the event plane may cost an operation at most one additional bounded background slice. `R = 1.25` allows 25 percent proportional growth on top of that.

#### A.3 Derivation formulas, fixed here

The ticket requires published p50, p95, p99, maximum, and throughput budgets. **Every one of them gates.** A recorded metric is not a budget.

Let `Pxcal_e(op)` and `Pxcal_d(op)` be the calibration percentile for the enabled and decoupled arms, and `THRcal_e(op)` / `THRcal_d(op)` the calibration throughput.

Latency budgets, one per percentile, all lower-is-better:

| Metric | Absolute budget from calibration | Relative acceptance gate |
| --- | --- | --- |
| p50 | `ABS50(op) = ceil_ms( P50cal_e(op) * 1.20 + S )` | `P50acc_e(op) <= P50acc_d(op) * R + S` |
| p95 | `ABS95(op) = ceil_ms( P95cal_e(op) * 1.20 + S )` | `P95acc_e(op) <= P95acc_d(op) * R + S` |
| p99 | `ABS99(op) = ceil_ms( P99cal_e(op) * 1.20 + S )` | `P99acc_e(op) <= P99acc_d(op) * R + S` |
| maximum | `ABSMAX(op) = ceil_ms( P99cal_e(op) * 3.00 + S )` | `MAXacc_e(op) <= MAXacc_d(op) * 3.00 + S` |

Throughput is higher-is-better, so its gate is a floor rather than a ceiling. The literal retention ratio is `T = 0.80`, which is exactly `1 / R`, so the same tolerance is expressed for both metric directions:

| Metric | Absolute floor from calibration | Relative acceptance gate |
| --- | --- | --- |
| throughput | `THRMIN(op) = floor_int( THRcal_e(op) * T )` | `THRacc_e(op) >= THRacc_d(op) * T` |

`ceil_ms` rounds up to the next whole millisecond. `floor_int` rounds down to the next whole operation per second. Ratios compute in `f64` and compare at three decimal places. Every rule in A.2 and A.5 — profile, warm-up, sample minimum, percentile method, outlier policy, phase separation, and failure handling — applies identically to all five metrics.

#### A.4 Two phases, in this order

1. **Calibration dispatch** on the reference runner. Its only outputs are a committed calibration dataset and the thresholds derived from it by the formulas in A.3. It produces no verdict about the product.
2. **Commit.** The calibration dataset and every derived threshold land in `docs/event-plane-load-proof.md` and `docs/reports/<slug>-calibration.json` as immutable literals. This commit must exist before acceptance starts.
3. **Acceptance dispatch**, a fresh dispatch on the same profile. Acceptance samples must never enter threshold derivation. Any attempt to re-derive a threshold from acceptance data invalidates the campaign.

#### A.5 Failure rules

Following the human answer, each of these **fails the campaign**. None is a caveat and none is residual risk:

- any recorded machine-profile field differs between calibration and acceptance;
- any required metric is missing from either phase;
- calibration is invalid, meaning fewer than `N` sessions reached steady state, fewer than 200 post-warm-up samples for any operation, or a `run-lifecycle-suite` verdict other than `clean`;
- any absolute, maximum, or relative threshold is breached;
- acceptance runs without a prior calibration commit, or against a different calibration commit than the one it records.

#### A.6 Also published in the same document

- The `N = 300` fleet target with its file-descriptor and PTY preconditions, and the rule that a shortfall is a `host_exhaustion` verdict plus escalation rather than a silent reduction.
- Every deterministic bound from section 4.2 and the router policy defaults, each named separately so a breach identifies its own limit ([[web event plane budgets are published numeric host limits]]).
- The recorded-signal list from section 12.4.
- The verdict vocabulary, reusing `clean`, `product_failure`, `host_exhaustion`, `environment_tainted`, and `survivors_present`.

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

### G. Downstream proof, wired rather than asserted

Plan Review `finding_1787268374_124555` is correct. Revision 1 claimed the campaign drives Web and TUI "through the existing `script/prove-north-star-shared-session` pattern" while the actual dispatch could not do so. The verified facts:

- `.github/workflows/loaded-daemon-lifecycle.yml` contains exactly two `actions/checkout` steps, at `:65-70` and `:92-98`. Neither sets `repository:`, so both check out `trybotster/botster-hub`. There is no Web, TUI, TUI Kit, Workspaces, Core, or Project Pipelines checkout anywhere in the file.
- `BOTSTER_WEB_CHECKOUT` and `BOTSTER_TUI_CHECKOUT` appear nowhere under `.github/`.
- The job installs Zig 0.16.0 (`:111-117`) and Rust 1.97.0 (`:119-122`). It installs **no Node or npm**.
- `script/run-loaded-daemon-lifecycle` accepts only `--subject-dir --artifact-dir --subject-sha --test-target --repetitions --stress-profile --validate-only --cleanup-only --help` (`:909-921`). It has no downstream-leg surface.
- **`script/prove-north-star-shared-session` has no Project Pipelines leg at all.** It admits only `botster-web` (`:641-650`) and drives the TUI through `script/test-live-hub`.
- `question.opened` appears **zero times** in this repository's code, tests, or scripts. Every hit is prose in `docs/plans/**`. The emit is owned entirely by `botster-project-pipelines`.
- `examples/project-pipelines` is a four-file fixture whose own README calls it a fixture. It is not the shipped package and is not a substitute for it.

#### G.1 The client harnesses cannot run this proof today

Revision 2 said no Web or TUI source change is needed. Plan Review `finding_1787270029_463152` rejected that, and direct verification of both repositories confirms the reviewer. Revision 3 withdraws the claim.

**botster-web** at `main` `71b461c20ccfe187bf2318773d791f168334fd18`, clean tree, in `scripts/live-packaged-protocol-harness.mjs`:

- The package-events lane is guarded at `:261` and calls `process.exit(0)` at `:275`. The shared-session lane only announces at `:299` and runs at `:355-530`. The package-events lane always exits first.
- `startWebrtcPackageRuntime` (`:8732`) branches at `:8733`; shared-session mode attaches to the caller socket and returns at `:8756`. The producer package is installed only on the isolated path at `:8783-8785`, unreachable from the caller-owned branch.
- Identity is hardcoded: `:1620` and `:1625` compare against `web-prod`, and the fixture binds `BOUND_SESSION_ID = "web-prod"` (`fixtures/package-events/plugin.lua:11`). The shared session is `north-star-shared`, so the join cannot match.
- The producer is a **local fixture**, driven through the plugin surface action `project-pipelines.events` (`emitPackageEventFixtureAction`, `:1547-1569`). `project_pipelines_ask_human` appears **zero times** in the repository.
- `BOTSTER_LIVE_PACKAGE_EVENTS` is absent from `exclusiveSharedSessionModes` (`scripts/live-packaged-protocol-helpers.mjs:473-480`), so setting both flags is not rejected; it silently takes the package-events branch and exits.

**botster-tui** at `main` `0032fe97c76bcaccb09e540247106a9a998c23c6`, clean tree:

- `script/test-live-hub:106-114` maps `ghostty-shared` to `ghostty_shared_attaches_to_caller_owned_hub_session`, and the shared branch at `:234-246` scrubs `BOTSTER_HUB_BIN` and `BOTSTER_SESSION_WORKER_BIN`. `:124-142` maps `package-events` to a test that builds its own `IsolatedHubBuilder` (`crates/botster-tui/src/app.rs:29677-29689`).
- The shared lane (`app.rs:22901-23146`) asserts only terminal-plane behaviour. It never references `PackageEvent`, `EventGap`, `question.opened`, or `SubscribeEvents`.

Two facts make the TUI gap smaller than the Web gap, and they matter for sequencing:

- The TUI production wire is already open on the shared lane. `try_connect` calls `subscribe_question_opened_events` (`app.rs:2289`) and `sync_entity_options_subscriptions` (`:2290`) unconditionally. Only assertion and producer are missing, not transport.
- The TUI package-events lane already drives the **real** producer, resolving the `ask_human` tool from the live MCP registry (`app.rs:29744-29748`) and calling `project_pipelines_ask_human` (`app.rs:29988-29996`), gated by `assert_project_pipelines_pin_floor` (`app.rs:29479-29489`) requiring `cd7c2f926fcead78e15e7a9c713ad26dfe883914` to be an ancestor of the supplied package HEAD. Web has no equivalent.

#### G.2 Two registered client dependencies

| Ticket | Repository | target_id | What it adds |
| --- | --- | --- | --- |
| `ticket_1787270342_754581` | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | a shared-session package-event lane on the caller-owned Hub, driven by the real Project Pipelines producer, with identity parameterised off `productionSessionId` |
| `ticket_1787270386_991884` | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | package-event assertions on the caller-owned shared lane, plus an endpoint-taking sibling of `call_plugin_tool` |

Both are registered against their own repository target, per [[cross repo dependency registration must use dependency repo target]]. Neither dependency **edge** is added yet: the engine blocks Plan Review while any edge is open, and human answer `question_1787267931_572353` requires the plan to be approved before dependency work starts. All three edges — these two plus `ticket_1787267568_492780` — are added together on approval, before any Implement advance. Section 14 records that obligation.

#### G.3 Campaign inputs and workflow additions

Three pinned inputs, each a full 40-character lowercase SHA, validated the way `script/test-production-package-runtime:300-322` validates revisions:

| Input | Repository | Consumed as |
| --- | --- | --- |
| `web_sha` | `trybotster/botster-web` | checkout at `$GITHUB_WORKSPACE/web`, exported as `BOTSTER_WEB_CHECKOUT`; must contain the `ticket_1787270342_754581` lane |
| `tui_sha` | `trybotster/botster-tui` | checkout at `$GITHUB_WORKSPACE/tui`, `submodules: recursive`, exported as `BOTSTER_TUI_CHECKOUT`; must contain the `ticket_1787270386_991884` lane |
| `project_pipelines_sha` | `trybotster/botster-project-pipelines` | checkout installed into the campaign Hub by path; **must be at or after `cd7c2f926fcead78e15e7a9c713ad26dfe883914`**, the TUI pin floor |

`BOTSTER_SHARED_SESSION_ID` is fixed to `north-star-shared`. `BOTSTER_HUB_BIN` and `BOTSTER_SESSION_WORKER_BIN` point at the binaries the existing precompile step already builds and provenance-checks at `:157-199`.

Workflow and coordinator additions:

1. Three `actions/checkout` steps with explicit `repository:` and `ref:`.
2. Node and npm setup, then `npm ci` and `npm run build` in the Web checkout, because `prove-north-star-shared-session:335-343` runs an npm script and installs nothing itself.
3. A Project Pipelines leg on `script/prove-north-star-shared-session`, which has none today: install the checked-out package by path and enable it, as `script/test-production-package-runtime:478-502` does. The package must be installed **exactly once** on the shared Hub; the client tickets defer to this leg.
4. The coordinator must bind a run to `BOTSTER_SHARED_SESSION_ID` and export its ids, because the TUI gates notices on `package_event_matches_active_run` (`app.rs:4062`, `:4104-4131`), which needs a `project-pipelines.session_request` record matching the selected session.
5. To keep gap coverage on the shared session, the coordinator must launch the shared Hub with `BOTSTER_ENV=test`, `BOTSTER_HUB_TEST_CLIENT_EVENT_QUEUE_MAX`, and `BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH`. Only the Hub launcher can set these, and [[hub client event queue max requires Botster test mode]] requires both values on the Hub child. Without them the shared lane can prove notice delivery and entity convergence but **not** `EventGap` shedding.
6. One step invoking the coordinator as a sibling of the loaded runner. Do not thread a downstream leg through `script/run-loaded-daemon-lifecycle`.

#### G.4 The production oracle for every acceptance condition

[[each acceptance condition names its authoritative production oracle]] requires the owning repository and the production API for each proof. [[cross-client acceptance uses one live session identity]] requires Web and TUI to share one caller-owned session.

| Acceptance condition | Owning repository | Authoritative production oracle |
| --- | --- | --- |
| A live `question.opened` reaches a client | botster-project-pipelines emits; Hub routes | the real package's `project_pipelines_ask_human` mutation, then `DaemonEvent::PackageEvent` on the host-control path. Not the `event-plane-producer` example, and not the Web `fixtures/package-events` stand-in |
| The durable question survives a shed notice | botster-project-pipelines | the package's entity plane, read as a `project-pipelines.question` entity record. [[a transient package event cannot be the sole authority for a durable close]] |
| Web shows one transient notice and keeps durable state | botster-web | the shared-session package-event lane added by `ticket_1787270342_754581`, reached through `drive:live-packaged-protocol:shared-session`. The lane does not exist today |
| TUI shows one transient notice and keeps durable state | botster-tui | the caller-owned shared lane extended by `ticket_1787270386_991884`, reached through `script/test-live-hub`. The assertion does not exist today |
| Web and TUI used the same live session | botster-hub | one caller-owned data directory and `BOTSTER_SHARED_SESSION_ID=north-star-shared`, with the coordinator's `attach_occupancy` barrier at `prove-north-star-shared-session:561-585` |
| Terminal input and output stay correct under saturation | botster-core owns the bytes; Hub observes only envelopes | the coordinator's marker sequence, plus the three Hub content-blindness architecture tests |
| Hub cannot inspect terminal bodies | botster-hub | `src/unix_terminal_adapter.rs:905`, `src/webrtc_terminal_adapter.rs:915`, `src/daemon_attach_stream.rs:1133` |
| Queue, shed, gap, latency, and timeout signals | botster-hub | the public daemon request added by dependency `ticket_1787267568_492780`. No client-side observation substitutes |
| Peer and session teardown reached idle | botster-hub | the owner-specific oracles in section 11.4, not `DaemonLifecycleCounters` alone |

#### G.5 Evidence shape

The campaign evidence JSON uses the seven-key `revisions` object from `docs/reports/bounded-hub-resources-fresh-campaign-evidence.json`, each value a flat 40-character SHA, plus the `provenance` block shape from `docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json` for the runner profile. The single-repo `inputs`/`provenance` shape alone is not sufficient, because it carries no downstream coordinate.

#### G.6 Operator precondition

Cross-repository checkout of the sibling repositories needs a credential. Today every checkout step sets `persist-credentials: false` and the job declares repo-scoped `permissions: contents: read`, which cannot read another repository. Implement must not invent a secret. If the sibling repositories are not public to this workflow's token, Implement stops and asks the operator for the exact token or App installation to use. This is recorded as unknown U6.

## 6. Repository ownership boundaries and cross-repo dependencies

### 6.1 Ownership

| Concern | Owner |
| --- | --- |
| Budget publication, campaign harness, verdict rules, evidence | botster-hub (this ticket) |
| Event-plane counters and their read path | botster-hub (dependency in 6.2) |
| Lifecycle journal, wake, pages, plugin admission classes | botster-core, unchanged |
| `question.opened` contract | botster-project-pipelines, unchanged |
| Transient-event consumption | botster-web and botster-tui. **Changed by two registered dependencies**, `ticket_1787270342_754581` and `ticket_1787270386_991884`; consumed from pinned checkouts that contain those lanes |
| Terminal bytes | Core `SessionIo` and `ClientWorker`, unchanged |

Hub must not gain terminal body access, Workspaces policy, or package product policy through this campaign. The existing architecture tests at `src/unix_terminal_adapter.rs:905`, `src/webrtc_terminal_adapter.rs:915`, and `src/daemon_attach_stream.rs:1133` stay in the gate list.

### 6.2 One same-repository blocking dependency to register

Registered: `ticket_1787267568_492780` against `tgt_7e208a0c76a44980a83b63af976b1f22` (botster-hub), linked as `dependency_1787267572_315049`. Same repository, so this is a sibling-surface dependency rather than a cross-repository one. It follows the project's established split, where `ticket_1787104273_140454` and `ticket_1786733177_803101` each supplied a surface before their consumer implemented against it.

Title: Hub: publish bounded event-plane observability counters and four load-campaign seams.

Contract:

1. Add monotonic, bounded, O(1)-per-event counters for shed by typed reason, client gap, admission attempts, and delivery attempts. Counters must not allocate or scan per event, following [[load diagnostics must not cost work proportional to what they measure]].
2. Add an oldest-queue-age value for each producer queue, each consumer queue, and each client mailbox. Age is already computed as a predicate; report the value.
3. Add bounded admission-latency and delivery-latency observations. A fixed-bucket histogram or a reservoir with a fixed cell count is acceptable. An unbounded sample vector is not.
4. **Add the timeout signal as four separately named counters, never merged**, matching section 4.4.1:
   - **T1** package-event handler invocation timeouts. This requires reading the discriminant that `run_completion_drain_slice` currently throws away. At `src/daemon_maintenance.rs:1322-1334`, match `PluginInvocationResult::Failed(failure)` and count by typed `PluginInvocationFailureKind`, so `TimedOut`, `HandlerFailed`, `Cancelled`, `Backpressured`, and `WorkerStopped` are each distinguishable. Retirement behaviour stays exactly as it is; only observation is added. This also closes a latent correctness gap, because a timed-out handler is currently indistinguishable from a successful one.
   - **T2** router queue-age expiries, counted where the envelope is retired at `src/package_event_router.rs:595-623`.
   - **T3** client-mailbox queue-age expiries, counted at `src/daemon_event_subscriptions.rs:266-270` and reported separately from queue-overflow gaps, so a gap's cause is identifiable.
   - **T4** add a real, separate transport-write-timeout counter. Classify the error at the two write-failure sites (`src/daemon_transport.rs:774` and `:1894`) as `ErrorKind::TimedOut` or not, carry that one classification on the internal `ControlMessage::EgressWriteFailed` beside `delivery_kind`, and count timeouts separately in `record_egress_write_failure` (`:3072`). `ControlMessage` is internal, so this needs no protocol version or conformance fixture bump. Keep `stalled_writes` unchanged as the existing all-write-failure total, and document that T4 is the timeout subset of it rather than a replacement.
   The authoritative source for T1 is Core's typed failure kind; Hub is the authoritative reporter. T2, T3, and T4 are Hub-owned throughout. All four are distinct counters; none is satisfied by an existing field.
5. Surface `last_owner_turn` and add a ready-operation wait measurement, then expose both through the existing `DaemonStatus` path rather than a new transport.
6. **The read path must not contend with `PackageEventRouter`'s mutex.** Counters read during saturation must not return `ShedBusy`. Use atomics beside the router, not a `try_lock` snapshot.
7. Add the four `BOTSTER_ENV=test` gated seams from section 5E, each as one environment read in `core_daemon_config` (`src/runtime.rs:4612-4641`) in the same style as `BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY`:
   - drop the next journal-advanced wake a bounded number of times;
   - set the lifecycle journal capacity, promoting the existing `#[cfg(test)]` `with_test_lifecycle_journal_capacity` (`src/runtime.rs:4598`) to an integration-reachable value;
   - set the package-event invocation `timeout_ms` used at `src/daemon_maintenance.rs:1121` and `:1274`;
   - hold a package-event handler for a bounded duration, so a handler can exceed the invocation timeout despite the Lua instruction budget.
8. Do not change any production budget, queue bound, or scheduling decision.

Acceptance for that dependency:

- every signal in the ticket's recorded-signal list is readable through a public daemon request, including all four timeout counters named separately;
- a focused test proves a package-event handler that exceeds `timeout_ms` increments the T1 `TimedOut` counter and no other kind, and that a handler failing without a timeout increments `HandlerFailed` instead. Both paths are indistinguishable today, so this test goes red before the change ([[a regression test must be shown to go red with the fix reverted]]);
- a saturation unit lane proves the counter read path still returns values while the router sheds;
- diagnostic cost is O(1) per event, proved by a work-bound test rather than a wall-clock test;
- the existing owner-turn and ready-operation invariants stay unchanged.

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

**A1 — "existing North Star budgets" do not exist as numbers.** Settled by human answer `question_1787268530_910910` (option A with an ownership limit). Section 4.4 Finding 2 establishes that the Terminal Transport North Star publishes behavioural oracles, not numeric input or output budgets, and that `docs/client-protocol.md:1189` explicitly disclaims performance targets. The answer's ruling:

1. The campaign publishes new numeric terminal input and output budgets **as event-plane coexistence regression budgets**. They state how much an operation may degrade when the event plane runs beside it.
2. They must **never** be described as pre-existing North Star budgets or as general terminal transport service levels. The final report must keep the two apart in its own words.
3. The campaign must also prove **every** North Star behavioural oracle under saturation — identity, ordering, exact bytes including non-UTF-8, late-attach history, resize, input, cancellation, reconnect, and `ProcessExited` — without changing terminal byte ownership or any terminal path.

Nothing is waived. A missing budget is created and correctly labelled rather than skipped.

**A2 — thresholds are fixed before calibration and cannot be chosen by the acceptance run.** Settled by human answer `question_1787268530_910910` (option E). Revision 1 left every absolute limit and the paired `R` and `S` for Implement, which made the central gate movable: the same run that measured could pick the threshold it passed. Section 5A now fixes the machine profile, workload, session count, warm-up, sample count, percentile method, derivation formulas, literal `R` = 1.25, literal `S` = 8 ms, rounding, outlier policy, invalid-run rules, and failure rules **before** calibration. Calibration and acceptance are separate dispatches; acceptance samples can never enter threshold derivation.

The vault rule that wall-clock durations are observations rather than gates ([[conformance harnesses gate on deterministic invariants not timing]]) is still respected where it matters. Scheduler and boundedness claims gate only on deterministic work-bound and decision-level oracles. The latency gate is a paired within-run ratio plus a calibrated absolute on one fixed profile, which removes the runner-speed dependence that rule exists to prevent.

**A3 — the reference runner is the existing loaded workflow.** `docs/loaded-daemon-lifecycle-runner.md` establishes a fresh GitHub-hosted `ubuntu-24.04` VM as the isolated campaign home. `script/run-loaded-daemon-lifecycle:141-144` forces `--stress-profile none` on Darwin, and `script/run-lifecycle-suite` refuses a dirty host. A developer machine that hosts a live Botster hub cannot produce this evidence. The declared machine profile is therefore the workflow runner.

**A4 — Web and TUI need real source changes; Project Pipelines does not.** Revision 2 claimed no client source change is needed. Direct verification of both repositories, recorded in section 5G.1, shows that claim was wrong, and revision 3 withdraws it. Neither client can today prove one real `question.opened` on a caller-owned shared session: the Web package-events lane exits before the shared lane and drives a local fixture rather than `project_pipelines_ask_human`, and the TUI shared lane carries no package-event assertion at all. Two dependencies are registered against their own repository targets, `ticket_1787270342_754581` for botster-web and `ticket_1787270386_991884` for botster-tui. botster-project-pipelines is consumed unchanged, from a pinned checkout at or after the TUI pin floor `cd7c2f926fcead78e15e7a9c713ad26dfe883914`.

**A5 — the baseline arm is projection-decoupled, not event-disabled.** The ticket permits either. Section 5C explains why the decoupled arm is chosen.

**A6 — `N = 300`.** "Hundreds" is satisfied at 300. The existing ceiling is 32, in an `#[ignore]` local test (`tests/hub_daemon_lifecycle/sessions.rs:5670`). 300 is a tenfold increase over any exercised count.

### Unknowns for Plan Review and Implement to resolve

**U1 — can the runner sustain 300 concurrent PTY sessions?** Each session consumes file descriptors and a PTY pair. `probe_fd_limit` and `probe_pty_allocation` (`tests/hub_daemon_lifecycle/harness.rs:244`, `:258`) already exist to classify exhaustion. Implement must measure the runner's `ulimit -n` and PTY ceiling first and record them in the published profile. If 300 is not admissible, the outcome is a `host_exhaustion` verdict and an escalation, not a silent reduction to a number that fits.

**U2 — `DAEMON_MAX_CONNECTIONS` is 64.** Sessions are not connections, so 300 sessions are admissible. The campaign must keep its client count well below 64 and must not confuse the two limits. The existing 64-connection saturation case (`tests/hub_daemon_lifecycle/sessions.rs:3324`) stays a separate concern.

**U3 — does natural saturation reach lifecycle cursor expiry?** `DEFAULT_LIFECYCLE_JOURNAL_CAPACITY` is 1024. Whether 300 churning sessions can outrun the Hub cursor by more than 1024 changes is unmeasured. The dependency seam removes the uncertainty; Implement should still record whether natural expiry occurred.

**U4 — the reported `EventPlaneSnapshot` read path.** Section 6.2 item 6 requires a read path that does not contend with the router mutex. Implement must confirm the dependency delivered that property before trusting any saturation-time queue reading.

**U5 — an existing budget test is nominal.** `published_owner_turn_budgets_fail_if_observe_walks_every_session` (`tests/session_projection_owner_loop.rs:207`) contains only `const` assertions and one discarded comparison. Despite its name it cannot fail if observe walks every session. The campaign must not cite it as owner-turn evidence. Whether to repair or rename it is a separate decision recorded as a vault gap in section 13.

**U6 — cross-repository checkout credentials.** Every checkout step in `.github/workflows/loaded-daemon-lifecycle.yml` sets `persist-credentials: false`, and the job declares repo-scoped `permissions: contents: read`, which cannot read a sibling repository. If `botster-web`, `botster-tui`, and `botster-project-pipelines` are not readable by this workflow's default token, Implement must stop and ask the operator for the exact token or GitHub App installation. Implement must not invent, guess, or widen a secret.

## 9. Affected surfaces and files

### New

| Path | Purpose |
| --- | --- |
| `docs/event-plane-load-proof.md` | published budget contract, machine profile, formulas, verdict rules |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | the campaign lanes |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json` | committed calibration dataset and derived immutable thresholds (section 5A phase 1) |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | narrative report; must keep coexistence budgets and the North Star behavioural contract distinct |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | machine-readable acceptance evidence, seven-key `revisions` object plus runner `provenance` |

### Modified

| Path | Change |
| --- | --- |
| `tests/hub_daemon_lifecycle/mod.rs` | register the new module |
| `script/run-loaded-daemon-lifecycle` | one new `--test-target event-plane-saturation` case beside the existing map at `:978-1024`. No downstream-leg surface is added here; that stays a sibling step |
| `.github/workflows/loaded-daemon-lifecycle.yml` | one new `options:` entry in the `test_target` choice list at `:14-30`; three new pinned SHA inputs `web_sha`, `tui_sha`, `project_pipelines_sha`; three new `actions/checkout` steps with explicit `repository:` and `ref:` (TUI with `submodules: recursive`); Node and npm setup plus `npm ci` and `npm run build` in the Web checkout; the coordinator invocation step |
| `script/prove-north-star-shared-session` | add the missing Project Pipelines leg: install the checked-out package by path and enable it, reading the name from its `botster-package.json`, in the style of `script/test-production-package-runtime:478-502`. Do not change the existing Web or TUI legs or the barrier sequence |
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
| R11 | The calibration threshold is chosen to fit the acceptance result | Section 5A fixes every parameter before calibration, splits the two dispatches, and requires the calibration commit to exist first. Acceptance records the calibration commit it gates against and a mismatch fails the campaign |
| R12 | Multi-repository wiring turns into an open-ended workflow project | Section 5G bounds it to three pinned SHA inputs, three checkouts, one toolchain addition, and one new coordinator leg. The loaded runner keeps its single-repo contract; the downstream leg runs as a sibling step |
| R13 | A missing credential silently degrades downstream proof to the example fixtures | U6 makes it a stop-and-ask. `examples/project-pipelines` is a four-file fixture and must never stand in for the shipped package, which is the only source of a real `question.opened` |
| R14 | The campaign asserts sibling survival on the fail-closed path and fails against correct code | Section 11.6 states the shipped policy and explicitly forbids that assertion. The campaign asserts the bounded blast radius instead |
| R15 | The campaign runs against client checkouts that lack the new lanes and silently proves nothing | Section 5G.3 requires `web_sha` and `tui_sha` to contain the two dependency lanes, and section 5G.4 marks both oracles as not existing today. A pinned SHA without the lane is a campaign failure, not a partial pass |
| R16 | The Project Pipelines pin is older than the TUI pin floor | Section 5G.3 requires `project_pipelines_sha` to be at or after `cd7c2f926fcead78e15e7a9c713ad26dfe883914`. `assert_project_pipelines_pin_floor` (botster-tui `app.rs:29479-29489`) already enforces it and will fail the lane |
| R17 | Shared-session gap coverage is silently dropped | Section 5G.3 item 5 requires the coordinator to launch the shared Hub with `BOTSTER_ENV=test` plus both queue and stall values. If it does not, the report must state that the shared lane covered notice delivery and entity convergence but not `EventGap` |

## 11. Runtime-teardown class

`teardown_class_applies`: **yes.** The campaign drives session and `ClientWorker` teardown at fleet scale, plugin worker restart, Unix and WebRTC client reconnect, and file-descriptor and PTY pressure. It also compares terminal state against live runtime state. [[botster runtime teardown lenses]] applies. Every required field is answered below against the current production code, not against an assumed design.

### 11.1 `teardown_isolation`

Ownership sets that die together:

| Failure | Ownership set removed |
| --- | --- |
| WebRTC peer close **succeeds** | that peer only: `peer_handlers` entry, `peers` entry, `peer_states`, and its grant-owned entity, event, and attach rows. Siblings are untouched. The runtime parks only when idle (`park_runtime_if_idle`, `src/local_webrtc.rs:264`). |
| WebRTC peer close **fails ultimately** | **every peer on the dedicated runtime.** See 11.6. |
| Client event holder | `(connection_id, subscription_id)` only. Connection cleanup must not remove a same-named holder on another connection ([[Client event holders are connection-scoped]]). |
| Session | its terminal subscriptions and attach routes, keyed `(session_id, subscription_id, generation)`. |

### 11.2 `teardown_bounds`

No unbounded control-plane wait. Existing bounds stay authoritative and unchanged:

| Bound | Value | Site |
| --- | --- | --- |
| `LOCAL_WEBRTC_PEER_CLOSE_BOUND` | 3 s production, 200 ms test | `src/local_webrtc.rs:60`, `:64` |
| `LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE` | 2 s (test oracle) | `src/local_webrtc.rs:68` |
| `DAEMON_CLIENT_WRITE_TIMEOUT`, `DAEMON_HANDSHAKE_TIMEOUT`, `DAEMON_INCOMPLETE_FRAME_TIMEOUT` | 2 s each | `src/daemon_transport.rs:152-154` |
| package-event invocation timeout | 1000 ms | `src/daemon_maintenance.rs:1121`, `:1274` |

Timeout on `peer.close()` is **treated as ultimate close failure**, not retried. The named hard stop is dropping the dedicated runtime: `fail_closed_drop_dedicated_runtime` (`src/local_webrtc.rs:333`) drops live peers, stale peers, and `self.runtime` without any further close wait. The code comment states the reason directly: sequential re-closes would each wait up to `LOCAL_WEBRTC_PEER_CLOSE_BOUND`, so N peers would make handler latency unbounded.

The existing oracle for this is `BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD` (`src/local_webrtc.rs:7175`), which re-execs a child so an ablated close timeout is finite-red rather than a stall. The campaign runs a close-hang lane at fleet scale so a hang under load fails a repetition instead of hanging the campaign.

### 11.3 `late_message_matrix`

Every ownership-creating surface, each with the existing production test that owns it. The generic "peer-originated request" row from revision 1 was wrong: the production suite treats **late Spawn** as its own ownership-creating surface.

| Message | Owner tag | Rejection after terminal failure | Residual sweep racing close | Existing production test |
| --- | --- | --- | --- | --- |
| `Spawn` | peer `grant_id` | a closed grant creates no session | grant sweep on `PeerClosed` | `local_webrtc_late_spawn_after_peer_closed_does_not_create_session` (`src/local_webrtc.rs:6619`) |
| `Attach` | `(session_id, subscription_id, generation)`, Core-owned | pre-`READY` failure creates no attach ownership | route-aware idempotent cleanup; occupancy uses the live attach route set | `local_webrtc_late_attach_after_peer_closed_does_not_recreate_state` (`:6544`); `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` (`:6887`) |
| `SubscribeEntities` | `grant_id` plus subscription id | closed grant does not recreate state | delayed snapshot must not delete a replacement owner's row | `local_webrtc_late_subscribe_entities_after_peer_closed_does_not_recreate_state` (`:5967`); `local_webrtc_stale_peer_snapshot_does_not_remove_replacement_subscription_owner` (`:6328`) |
| `UnsubscribeEntities` | `grant_id` plus subscription id | applies only to the calling grant | must not delete a replacement owner's row | `local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row` (`:6674`) |
| `SubscribeEvents` | `(connection_id, subscription_id)`; Unix `client_id`, WebRTC `grant_id` | closed connection admits no holder | connection cleanup drops only that connection's holders | **campaign adds this lane**; no existing late-message test covers event holders |
| `UnsubscribeEvents` | same | applies to the calling connection only | same | **campaign adds this lane** |
| Admitted event holder | producer plus Core job id | producer unload removes contracts and queued copies | `AdmittedHolder` survives until `CompletionDrain`; late completion is idempotent and cannot drive bytes below zero | [[admitted event holders survive producer unload until Core completion]] |

The campaign runs every row at fleet scale, in **both** queue orders (closed-first and message-first), with **deliberately reused subscription and grant ids**. The two event-holder rows are new coverage this campaign contributes; the rest re-run existing production oracles under saturation.

### 11.4 `production_path_proof`

The exact production chain, named rather than described. `src/local_webrtc.rs:5539` documents it verbatim:

> `LocalWebrtcPeerClosed` → `handle_control_message` → `remove_peer` close + map + runtime drop.

- `ControlMessage::LocalWebrtcPeerClosed` is emitted at `src/local_webrtc.rs:1031`.
- `handle_control_message` dispatches it at `src/daemon_transport.rs:2874` and `:5334`.
- `remove_peer` (`src/local_webrtc.rs:252`) is documented as "the sole production forget path for `LocalWebrtcPeerClosed`". It calls `close_peer_on_runtime`, then either `take_remove_result` plus `park_runtime_if_idle`, or `fail_closed_drop_dedicated_runtime`.

Owner-specific live idle oracles, not a process-wide thread ceiling:

| Oracle | Site | Proves |
| --- | --- | --- |
| `active_peer_count()` | `src/local_webrtc.rs:480` | the live peer map is empty |
| `has_live_peer(grant_id)` | `:311` | one exact grant is gone |
| `peer_state_count()` | `:511` | primary and sibling `peer_states` are cleared |
| `has_dedicated_runtime()` | `:485` | the runtime was dropped, not merely parked |
| `dedicated_runtime_worker_threads()` | `:505` | **each peer driver is idle.** This counter is instance-local on `LocalWebrtcTransport`, which [[process-global test counters make zero waits observe other tests under default-concurrency lib load]] requires |

A terminal record is not teardown proof. [[terminal webrtc failure records do not prove peer runtime teardown]] records the case where `local-webrtc-sender-terminal.json` showed `peer_failed` with completed cleanup while the process ran at roughly 500% CPU for 23 hours on live `PeerConnectionDriver` timeout loops. The campaign therefore asserts the live oracles above, and uses `BOTSTER_ASSERT_IDLE_CPU_BOUND` only as corroboration.

Hub-wide corroboration stays secondary: `DaemonLifecycleCounters` returning to baseline, `script/probe-hub-resources` converging, and the runner's owned-session, owned-token, and settled-zombie censuses empty.

### 11.5 `ownership_identity`

Every peer-created durable row carries the stable owner id in the 11.3 table. A delayed `PeerClosed` snapshot must not delete a row now owned by a different live peer that reused a subscription id. Two existing tests already own that claim (`:6328`, `:6887`) and the campaign re-runs both under saturation with reused ids in both queue orders.

### 11.6 `sibling_fail_closed_policy`

**Correction to revision 1.** Revision 1 stated that no sibling is sacrificed. That contradicted the shipped production path. The actual policy is a deliberate, bounded sibling sacrifice.

- **On successful close:** siblings keep working. `remove_peer` removes one grant and parks the runtime only when idle. The campaign asserts sibling survival at fleet scale after every non-fatal fault.
- **On ultimate close failure (error or `LOCAL_WEBRTC_PEER_CLOSE_BOUND` timeout):** Hub **fail-closes and sacrifices every sibling on the dedicated runtime.** `fail_closed_drop_dedicated_runtime` (`src/local_webrtc.rs:333`) takes all of `self.peers` and all of `self.stale_close_peers`, adds the primary grant, sweeps ownership for every one of them, and drops the runtime.

**Blast radius, stated exactly:** every live peer on the dedicated runtime, every stale close peer, the failed primary grant, all of their `peer_states`, all of their grant-owned entity, event, and attach ownership rows, and the dedicated tokio runtime itself. Peers on other transports and all Unix connections are unaffected.

**Why the tradeoff was chosen:** the alternative is sequential per-peer re-close, where each peer can wait up to `LOCAL_WEBRTC_PEER_CLOSE_BOUND`, making `PeerClosed` handler latency scale with peer count. Unbounded control-plane latency is the worse failure, and leaving a failed peer on a runtime kept alive by siblings is what produced the original multi-core timeout storm. This is the documented tradeoff the teardown lens asks a plan to state rather than hide.

**Existing test that owns the policy:** the fail-closed hang path at `src/local_webrtc.rs:7117-7147` asserts `active_peer_count() == 0`, `!has_dedicated_runtime()`, `peer_state_count() == 0`, absence of both `grant_a` and `grant_b` with the message "timeout fail-closed must sacrifice sibling peers", and removal of both sibling entity subscriptions.

**Campaign obligation:** run this exact path at fleet scale and assert the full blast radius above, plus the handler-join bound. The campaign must **not** assert sibling survival on the ultimate-failure path; that assertion would contradict shipped behaviour and fail. Changing this policy to per-peer isolation would require a separate owner ticket that redesigns the shared dedicated runtime, and is out of scope here.

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
10. **North Star behavioural oracles hold under saturation** on the attached noisy session: identity, ordering, exact non-UTF-8 bytes, late-attach history, resize, input, cancellation, reconnect, and `ProcessExited`. These are the pre-existing behavioural contract, proved unchanged; they are not the new coexistence budgets.
11. **Teardown matrix.** Every row of section 11's late-message matrix, in both closed-first and message-first orders, with reused subscription ids.
12. **Sibling survival after each non-fatal fault**, at full fleet size. On the ultimate WebRTC close-failure path the campaign asserts the **bounded sibling sacrifice** in section 11.6 instead, because asserting survival there would contradict shipped behaviour.
13. **Downstream production proof**, per section 5G.3: a real `question.opened` from the pinned `botster-project-pipelines` checkout produces exactly one transient notice in the shipped Web harness and one in the shipped TUI harness, both attached to the same `north-star-shared` session; a shed notice never removes the durable question row; and reconnect replays nothing.

### 12.4 Published budget gates

For each of `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, `Shutdown`, MCP, UI, entity, terminal input, and terminal output, in both arms and in both phases:

- record p50, p95, p99, maximum, and throughput under the fixed nearest-rank method in section 5A.2;
- **gate every one of the five metrics**, absolute and relative, exactly as section 5A.3 defines. p50, p95, p99, and maximum gate as ceilings; throughput gates as a floor at `T` = 0.80. None of the five is recorded-only;
- record queue count and bytes, oldest age, admission latency, delivery latency, shed by typed reason, gap count, resync count, pressure, **the four timeout counters T1 through T4 from section 4.4.1, each a distinct counter**, owner-turn duration, and ready-operation wait.

The recorded-signal list requires dependency `ticket_1787267568_492780`. Without it these signals have no read path and the ticket cannot pass.

Every failure rule in section 5A.5 applies. A profile mismatch, a missing metric, an invalid calibration, or any threshold breach on any of the five metrics fails the campaign. None of these is a caveat.

### 12.5 Fault lanes

Each of the eleven faults in section 5E runs at full fleet size. Each asserts that the typed result is the expected one, that no queue exceeds its bound, that durable state converges, and that no operation blocks.

The sibling assertion is **not** uniform, because the shipped policy is not uniform. Section 11.6 governs it:

| Fault class | Sibling assertion |
| --- | --- |
| The ten non-fatal rows of the section 5E matrix: full router ingress, router contention, client mailbox full, plugin mailbox full, plugin worker restart, Unix reconnect, dropped lifecycle wake, lifecycle cursor expiry, slow handler, handler timeout | siblings survive and keep working |
| The eleventh row, WebRTC client reconnect, where close **succeeds** | siblings survive; the runtime parks only when idle |
| The same eleventh row driven to **ultimate close failure**, meaning an error or a `LOCAL_WEBRTC_PEER_CLOSE_BOUND` timeout | **assert the bounded sacrifice, not survival.** Every peer on the dedicated runtime, every stale close peer, and the failed primary grant are swept and the runtime is dropped. Also assert the unaffected boundary: peers on other transports and every Unix connection keep working, and the handler stays within its join deadline |

A blanket "siblings survive" rule across all eleven faults would contradict shipped behaviour and fail against correct code. Risk R14 records that trap.

### 12.6 Ablation, the red-on-revert requirement

[[a regression test must be shown to go red with the fix reverted]] requires per-claim negative controls, because an early failure can stop later assertions. The campaign records an ablation truth table for at least:

- the non-blocking ingress claim, ablated by making `events.emit` wait;
- the queue-bound claim, ablated by removing one bound;
- the gap-plus-baseline convergence claim, ablated by dropping the resync;
- the terminal content-blindness claim, ablated by naming a snapshot phase in an adapter.

The report names, per claim, which tier detected the defect and which tier was blind, following [[verification reports name the load bearing oracle when cheaper suites are blind]].

### 12.7 Campaign dispatch, two phases

Phase 1, calibration. Its only outputs are the committed dataset and derived thresholds.

```sh
gh workflow run loaded-daemon-lifecycle.yml \
  --ref main \
  -f subject_sha=<exact merged Hub SHA> \
  -f web_sha=<pinned botster-web SHA> \
  -f tui_sha=<pinned botster-tui SHA> \
  -f project_pipelines_sha=<pinned botster-project-pipelines SHA> \
  -f test_target=event-plane-saturation \
  -F repetitions=<published> \
  -f stress_profile=residual-tail
```

Phase 2, acceptance. Identical inputs, dispatched only after the calibration commit lands. Acceptance records the calibration commit SHA it gates against; a mismatch fails the campaign.

The runner stops at the first red repetition. Preserve that artifact before any further dispatch. Green repetitions at a bounded budget mean "not reproduced under that budget", never "resolved". Do not use a retry loop that discards a red result.

## 13. Vault gaps worth capturing

1. **The Terminal Transport North Star publishes oracles, not numeric budgets.** Downstream tickets already assume "existing North Star budgets" exist. Capture the correction and name this campaign as the first publisher of terminal numbers.
2. **The Hub event plane has no latency, age, shed-count, or gap-count observability.** Capture the exact absence list from section 4.4 so no later plan assumes these are readable.
3. **`EventPlaneSnapshot` is unreadable exactly under saturation** because `PackageEventRouter::snapshot` uses `try_lock`. Capture this as a diagnostic-design rule: a saturation-time counter must not compete for the lock whose contention it reports.
4. **`published_owner_turn_budgets_fail_if_observe_walks_every_session` is nominal.** Capture that a test name can assert a claim its body cannot fail on, and that budget claims need a work-bound body.
5. **Only four of Core's fourteen `with_test_*` builders are plumbed to Hub environment reads.** Capture the inventory so future fault lanes reach for the existing builder before inventing a seam.
6. **`DAEMON_MAX_CONNECTIONS` 64 is a client bound, not a session bound.** Capture the distinction, because "hundreds of sessions" invites the confusion.
7. **A timed-out package-event handler is indistinguishable from a successful one.** `run_completion_drain_slice` (`src/daemon_maintenance.rs:1322-1334`) reads `completion.result` only to extract a request id and never inspects the `Completed` versus `Failed` discriminant on the event path. Hub also never references `PluginWorkerEvent` anywhere, so Core's typed `InvocationTimedOut` is unobservable by construction. Capture this as a correctness gap, not only an observability gap.
8. **The loaded workflow is structurally single-repository.** It has two `actions/checkout` steps, both Hub, and no Node or npm. `script/prove-north-star-shared-session` has no Project Pipelines leg, and `question.opened` appears zero times in this repository's code. Capture that a plan claiming downstream proof through that workflow must wire three checkouts, a toolchain, and a new coordinator leg first.

## 14. Pipeline gates and artifacts

| Item | Value |
| --- | --- |
| Gate | `botster_stack_plan_gate` |
| Plan artifact | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md`, revision 3 |
| Vault checklist | `checklist_1787266824_449406` |
| Plan Reviews answered | `review_1787268374_271226` (6 findings) and `review_1787270029_776949` (4 findings) |
| Human answers folded in | `question_1787267931_572353` (routing exception), `question_1787268530_910910` (budget nature and derivation) |
| Delivery | direct merge into `main`; no pull request; no human pull-request sign-off |

### 14.1 Three dependency tickets, all edges deferred

| Ticket | Repository | target_id | Status |
| --- | --- | --- | --- |
| `ticket_1787267568_492780` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | open, unstarted |
| `ticket_1787270342_754581` | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | open, unstarted |
| `ticket_1787270386_991884` | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | open, unstarted |

**No dependency edge is registered.** The engine blocks every advance while any edge is open, including the advance to Plan Review, and `override_unmet_gates` does not cover ticket dependencies. Human answer `question_1787267931_572353` established the review-only exception for exactly this reason and requires the plan to be approved before dependency work starts. Because both Plan Reviews so far requested changes, its condition 3 applies: the tickets stay unstarted and the edges stay absent.

**On Plan Review approval, before any Implement advance:**

1. Add all three edges with `project_pipelines_add_ticket_dependency` against `ticket_1786663585_879846`.
2. Run and merge `ticket_1787267568_492780` first; the campaign cannot record seven of its twelve signals without it.
3. Run and merge `ticket_1787270342_754581` and `ticket_1787270386_991884`; they can proceed in parallel with each other.
4. Only then start Implement here, with `web_sha`, `tui_sha`, and `project_pipelines_sha` pinned to revisions that contain those lanes and satisfy the pin floor in section 5G.3.

This integration run stays parked throughout.
