# Plan: prove the event plane cannot lag or block Hub operations

Ticket: `ticket_1786663585_879846`
Run: `run_1787262311_549251`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Project: `project_1786663508_823105` Botster Non-Blocking Event Plane, Stage D
Vault checklist: `checklist_1787266824_449406` (ticket scope, one Plan visit)

## Plan revision 11

Revision 11 records Review `review_1787440703_182294`. Gate 4 records every
post-warm-up attempt at start and still records the completion if it finishes
after the window. Late successful completions keep Gate 4 passing when
attempts equal successes. An incomplete cycle, attempts≠successes, timeout,
disconnect, incomplete response, or worker error fails Gate 4. Gate 2 uses
only `window_completed` and does not treat worker errors as the window proxy.

Gate 5 stores structured terminal oracles: exact bytes and ordering,
continuous sequence, zero I/O failure, zero unexpected terminal gap, and no
peer loss. PackageEvent or EventGap on Unix/WebRTC event subscriptions remain
valid event-plane observations, not Gate 5 failures. Each oracle has a
negative test.

The scheduler-lag budget is **at most** 50,000 µs: 50,000 passes and only
values strictly greater fail. An arm panic or early failure keeps partial
`ArmRun` evidence, stops workers and the watchdog, classifies available
gates, persists `event-plane-host-validity.json`, and fails with that class.
Gates 1–5 are `fail` rather than `not_evaluated` when the arm started and
then aborted.

## Plan revision 10

Revision 10 records Review `review_1787439371_225335` plus human answer
`question_1787439231_161941` (restated as `question_1787439421_445099`).
Replace “published operation and terminal budgets” as the disabled-arm
validity rule with eleven immutable pre-calibration gates. Do not invent
pre-calibration `ABS*` or `THRMIN`. Do not derive a disabled-arm latency
threshold from the same calibration run.

Disabled-arm host validity requires all of: N=300 at steady state; the full
600-second window; ≥200 post-warm-up samples per operation; zero operation
failures; terminal behavioural oracles; `max_owner_turn_us` ≤ 25,000 µs;
`max_ready_operation_wait_us` ≤ 50,000 µs; monotonic scheduler-lag maximum
≤ 50,000 µs measured by a watchdog across the disabled-arm interval;
applicable queue age ≤ 1,000,000 µs; no confirmed FD or PTY exhaustion; no
environment taint or survivors.

Host-exhaustion evidence is scheduler-lag maximum and confirmed FD/PTY.
Owner-turn, ready-wait, queue-age, sample, and terminal failures without
that evidence are `product_failure`. Gate 11 is `environment_tainted` or
`survivors_present`. Load average never selects the verdict. Every
host-validity classification, including pre-measurement and early exits,
persists a bounded artifact with lag, load averages, runnable count, total
threads, CPU steal with units, FD/PTY, every gate result, and the final
class.

## Plan revision 9

Revision 9 records the Verify-return amendment from `question_1787428441_900918` / `question_1787437854_708832`. Residual-tail calibration dispatches `32591282234` and `32591872269` at `ef77621`, and `32594580606` at `8ee0d7a`, are **inconclusive `host_exhaustion` observations**, not product pass/fail. Captured counters from `32594580606` (`max_owner_turn_us` 219723, `max_ready_operation_wait_us` 1845228) must not be cited as product results.

The published reference for this campaign is now `stress_profile=none` on the same GitHub-hosted `ubuntu-24.04` four-CPU runner. `N=300`, 300 quiet PTYs, the noisy 4 KiB / 100 ms producer, four drivers, 150 events per second, and enabled-versus-disabled comparisons stay fixed. Residual-tail remains the default for other loaded-lifecycle tickets.

The campaign classifies host validity from the eleven immutable
pre-calibration gates in revision 10. A 25 ms busy-spin is not the
disabled-arm scheduler-lag measurement; the watchdog records the monotonic
maximum across the full disabled-arm interval and is stopped and joined on
teardown.

The ShedBusy fault lane asserts the typed `EventPlaneStatus::ShedBusy` result. It does not use a wall-clock bound.

## Plan revision 8

Revision 8 answers Plan Review `review_1787278903_443047` (`changes_required`, three findings). All three were correct.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787278903_138982` The named generic client fixture has no event-plane coverage | **blocker / product** | I cited `run_client_conformance` (`crates/botster-hub-test-support/src/lib.rs:1440`) without reading its body. It proves status, sessions, attach, input, resize, drain, and lifecycle, and contains no `SubscribeEvents`, `UnsubscribeEvents`, `PackageEvent`, or `EventGap` path; those symbols appear nowhere in that crate. New section 5G.1.1 names the six Hub-local proofs in `tests/hub_daemon_lifecycle/package_event_plane.rs` that already enter through the public client boundary, and this ticket promotes them into a new repository-owned `run_client_event_conformance` in `crates/botster-hub-test-support/src/lib.rs`, driven under saturation. Section 9 lists the changed file and the published-surface check; section 12.3 item 13 cites the new entrypoint. |
| `finding_1787278903_125669` The prerequisite graph hides two active public-contract dependencies | high / product | Section 5G.3 now carries the full five-ticket graph with targets, deliverables, and edge ids, including `ticket_1787278643_145174` (Hub package-owned client notice reaction descriptor in `@trybotster/ui-contract`, `HubPackageManifest`, and `DaemonPackage`) and `ticket_1787278658_151737` (Project Pipelines declaration plus `payload.subject`). Both are named as material public-contract changes. It also corrects revisions 6 and 7: Project Pipelines is **not** unchanged; a prerequisite in that repository changes it, while this campaign neither changes nor executes it. New unknown U7 covers whether the descriptor forces a declaration on this campaign's own saturation fixtures. |
| `finding_1787278903_320205` The consumption and revision evidence rules contradict each other | high / product | Sections 5F, 5G.3, 5G.6, 7, and 14.1 stated four incompatible rules. Section 5G.6 is now the single authority and splits **executed revisions** (Hub and its locked Core, the only code this workflow runs) from **cited prerequisite revisions** (the five prerequisites' merged revisions and gate artifact ids, explicitly not executed). Sections 5F, 6.1, 7, and 14.1 defer to it, and the non-scope wording now scopes "edits and executes" separately from "is a prerequisite" so the two no longer read as a contradiction. |

## Plan revision 7

Revision 7 folds in the operator instruction that followed revision 6: two library-cleanup tickets now exist and are running, and this campaign consumes three repository-owned prerequisites rather than one.

| Change | Detail |
| --- | --- |
| Three prerequisites, not one | `ticket_1787267568_492780` (botster-hub observability), `ticket_1787278327_274484` (botster-web), and `ticket_1787278327_199618` (botster-tui). Section 14.1 carries the table, the run ids, and the edge handling. |
| Why the client tickets are prerequisites | Section 5G.3. Both clients today hardcode the Project Pipelines owner, `question.opened`, its payload, and its workflow entity families in **production** code. Until that is removed, a Hub-side generic boundary proof would be representative of nothing that ships, because the only real consumers would still be product-coupled. The cleanups also produce the client-side mirror of this campaign's Hub-side fixtures: neutral contract fixtures that enter through the public protocol boundary and do not inject after protocol decoding. |
| Consumed as merged state | The campaign checks out no client repository, installs no product package, and drives no client harness. Section 5G.5 keeps the workflow single-repository. |
| Superseded tickets stay closed | `ticket_1787271303_548807`, `ticket_1787270342_754581`, and `ticket_1787270386_991884` are not restored. They added product coupling; the two running client tickets remove it. They are opposites, not replacements. |
| New risk | R19: running the campaign against client revisions that predate the cleanups would assert the boundary claim while the shipped clients are still product-coupled. The evidence must record that the run happened after all three merged. |

Assumption A4 is rewritten accordingly: the clients are made generic by prerequisite tickets rather than assumed generic.

## Plan revision 6

Revision 6 answers Plan Review `review_1787278015_433684` (`changes_required`, one blocker). That review supersedes the approval `review_1787272071_523159` because the human rejected the product-specific dependency chain that revisions 3 and 4 built.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787278015_548510` Product-specific shared-session proof violates the library boundary | **blocker / product** | Revisions 3 and 4 required the real `botster-project-pipelines` package, a bound product run, one shared `north-star-shared` session, and new product lanes in botster-web and botster-tui. That chain is removed. Section 5G is rewritten around repository-owned generic contract fixtures at public boundaries: `examples/event-plane-producer` and `event-plane-consumer` through the real package ABI and the real `PackageEventRouter`, `examples/event-plane-cycle` for causal scope, `fixtures/plugins/plugin-contract-matrix` for the ABI surface, and `run_client_conformance` (`crates/botster-hub-test-support/src/lib.rs:1440`) for generic Unix and WebRTC client consumption. Section 5G.3 cites the canonical contract proof each owner repository keeps, with `question.opened` staying inside botster-project-pipelines. Section 5G.4 removes the shared-session identity requirement. Section 5G.5 keeps the loaded workflow single-repository, which retires unknown U6 and risks R13, R15, R16, and R17. Section 14.1 reduces the prerequisite set to `ticket_1787267568_492780` alone and records the three superseded tickets. |

**Why the reduction is right, not merely instructed.** Botster components are libraries. A Hub load campaign that could only be proved by installing one first-party product package, binding one product run, and editing two client repositories was proving one product configuration rather than a library boundary. `docs/client-protocol.md:1374` already states the boundary this revision follows: an external client depends on the client protocol crate plus the test-support crate, not on the full `botster-hub` library. Revisions 3 and 4 reached across that line; revision 6 stops at it.

**What survives unchanged**, because the reviewer confirmed it may: the numeric budgets and all five gated metrics (section 5A), the fully fixed saturation workload and the failed-attempt rule (5A.2, 5A.2.1), the observability dependency (6.2), the runtime-teardown answers (section 11), and the failure rules (5A.5).

**What the earlier rounds still bought.** The client-repository evidence gathered in revisions 3 and 4 remains true and is captured in the vault. Neither client can prove a real `question.opened` on a caller-owned shared session today. That no longer blocks this campaign, but it is exactly what whoever owns the cross-client `question.opened` contract will need, and it will not have to be rediscovered.

## Plan revision 5

Revision 5 answers Plan Review `review_1787271799_830342` (`changes_required`, one finding, no blocker). The finding was correct.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787271799_979271` Failed operation cycles can be omitted without failing acceptance | high / product | Revision 4 said a failed cycle contributes no latency sample and stopped there. That let a failed `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, MCP, UI, entity, or `Shutdown` vanish from latency and only reduce throughput, where the `T` = 0.80 floor could absorb up to a fifth of the work, and a calibration run could bake its own failures into a low floor. New section 5A.2.1 records every attempted operation with its outcome, reports attempts, successes, and failures per operation, and makes any unexpected error, request timeout, disconnect, incomplete response, or incomplete cycle an **immediate `product_failure` in calibration and acceptance alike**. Percentiles still use successful samples only, which is now safe because one failure already fails the run. Section 5A.5 and section 12.4 carry the rule, and R18 records the trap. Measurement arms run no fault injection, so typed shed and gap results remain expected only inside the section 12.5 fault lanes, never on the nine measured operations. |

## Plan revision 4

Revision 4 answers Plan Review `review_1787271188_552110` (`changes_required`, three findings). All three were correct.

| Finding | Class | Correction |
| --- | --- | --- |
| `finding_1787271188_172074` Client dependencies require a coordinator that cannot exist before they merge | **blocker / product** | A real deadlock I created in revision 3. The Web and TUI tickets deferred package installation and run binding to a coordinator leg planned inside this ticket, while section 14.1 forbade this ticket from starting Implement until those clients merged. Revision 4 takes the reviewer's first option and adds `ticket_1787271303_548807`, a botster-hub ticket that ships the coordinator leg first and proves itself with a **Hub-side** subscriber, so its acceptance needs neither client. The two client tickets now carry registered engine edges onto it (`dependency_1787271311_659291`, `dependency_1787271318_253833`). The graph in section 14.1 is acyclic. The reviewer's second option, a self-contained driver per client, was rejected because two drivers each starting their own Hub cannot satisfy [[cross-client acceptance uses one live session identity]] and would duplicate install logic across two repositories. |
| `finding_1787271189_975860` The calibration workload and throughput measurement are not fixed | high / product | Section 5A.2 now fixes the whole workload, not just the actors and session count: the 10-wave spawn ramp, the steady-state trigger, driver concurrency 4, the exact 9-operation cycle and its order, no think time, the 600-second measurement window, the window-boundary rule, the event schedule at 150 per second in 25-event bursts with a 4 KiB payload, the terminal cadence of 4 KiB every 100 ms out and 64 bytes every 500 ms in, the latency-sample definition, and the throughput numerator and denominator. Section 5A.5's profile-mismatch rule now covers **every** value in 5A.2, not only the machine fields. Section 5C's "at and above configured limits" is replaced by the fixed schedule. |
| `finding_1787271189_232353` Non-scope still says Web and TUI source do not change | high / product | The non-scope bullet contradicted 5G.2, 6.1, A4, and 14.1. It now states the two facts separately: this Hub run edits no client repository, and botster-web and botster-tui do need source changes owned by their own registered tickets, which must merge before this run starts Implement. |

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
| Spawn-target name | `botster-hub` (authoritative spawn-target path; path-neutral) |
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
- [[event plane client proof uses library contract fixtures]] -- the governing note for the library-boundary shape; Hub owns a fixture plugin over the public package ABI and router, clients own their canonical protocol harnesses, and each product plugin proves its own emitted contract
- [[question opened clients subscribe with empty subjects]] -- being superseded by `ticket_1787278658_151737`, which replaces client-side workflow filtering with subject targeting

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

Every value below is fixed by this reviewed plan. Neither calibration nor acceptance may change any of them, and **every one is part of the profile-mismatch failure rule in A.5**, not only the machine fields. Two runs whose machine fields match but whose workload differs are a mismatch.

**Machine profile**

| Parameter | Fixed value |
| --- | --- |
| Runner | fresh GitHub-hosted `ubuntu-24.04` from `.github/workflows/loaded-daemon-lifecycle.yml` |
| Recorded fields | runner image, architecture, CPU count, total memory, kernel release, `ulimit -n`, PTY ceiling, Rust 1.97.0, Zig 0.16.0 |
| Stress profile | `none`, identical in both phases |

**Fleet**

| Parameter | Fixed value |
| --- | --- |
| Background sessions `N` | 300 quiet sessions |
| Spawn ramp | 10 waves of 30, 200 ms between waves |
| Steady state | declared when all 300 report `running`; the warm-up clock starts then |
| Attached noisy PTY | exactly 1, held for the whole run |

**Measured operation schedule**

| Parameter | Fixed value |
| --- | --- |
| Driver concurrency | 4 workers, closed loop, well below `DAEMON_MAX_CONNECTIONS` 64 |
| Cycle | each worker repeats one fixed 9-operation cycle in this exact order against its own churn session: `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, MCP, UI, entity read, `Shutdown` |
| Samples per cycle | exactly one per operation. Every attempt is recorded with its outcome. A latency sample is kept only for a successful operation, but a failure is never merely dropped: see A.2.1 |
| Think time | none; the next cycle starts when the previous one ends |
| Measurement window | 600 seconds of steady state after warm-up |
| Warm-up | the first 30 seconds of steady state **and** the first 20 samples of each operation, whichever ends later |
| Minimum samples | 200 post-warm-up samples per operation per arm; fewer invalidates the run |
| Window boundary | an operation counts only if it both starts and finishes inside the window. Operations crossing either edge are excluded from latency and from throughput |

**Event emission, enabled arm only**

| Parameter | Fixed value |
| --- | --- |
| Sustained rate | 150 events per second, deliberately above `package_rate_per_sec` 100 so the token bucket rejects and shed is guaranteed rather than incidental |
| Burst shape | 25 events every 1/6 second, six bursts per second |
| Payload | fixed 4 KiB, well under the 64 KiB cap, so payload size is never the shed cause |
| Duration | continuous for warm-up plus the full measurement window |
| Subscribers | one `event-plane-consumer` plugin, one Unix client, one WebRTC client |

**Terminal cadence, both arms, identical**

| Parameter | Fixed value |
| --- | --- |
| Output | the noisy PTY emits one fixed 4 KiB line every 100 ms, so 40 KiB per second |
| Input | the driver sends one 64-byte line every 500 ms |

**Measurement definitions**

| Term | Fixed definition |
| --- | --- |
| Latency sample | wall time from request submission to response receipt, measured by the driver |
| Percentile method | nearest-rank on the ascending sample vector, no interpolation. For `n` samples, `p` is the sample at 1-based index `ceil(p * n)` |
| Throughput numerator | post-warm-up successes, including completions that finish after the window closes |
| Throughput denominator | the fixed 600-second window, in seconds |
| Throughput unit | completed operations per second |
| Literal `R` | **1.25** |
| Literal `S` | **8 ms** |
| Literal `T` | **0.80** |
| Rounding | derived millisecond thresholds round **up**; derived throughput floors round **down**; ratios compute in `f64` and compare at three decimal places |
| Outlier policy | **none.** No post-warm-up sample is discarded |

The decoupled arm is identical in every row above except that no package is admitted, no emitter runs, no plugin subscribes, and no client holds an event subscription. Fleet, ramp, cycle, concurrency, window, warm-up, and terminal cadence are the same values, so the paired ratio compares like with like.

`S = 8 ms` is derived, not chosen. It is exactly `EVENT_DELIVERY_MAX_ELAPSED` (`src/daemon_maintenance.rs:1158`), which equals `OBSERVE_SLICE_BUDGET.max_elapsed` and `BASELINE_PAGE_BUDGET.max_elapsed`. The slack therefore states that the event plane may cost an operation at most one additional bounded background slice. `R = 1.25` allows 25 percent proportional growth, and `T = 0.80` is exactly `1 / R`.

#### A.2.1 Every attempted operation is recorded, and any failure fails the campaign

Revision 4 said a failed cycle contributes no latency sample and stopped there. Plan Review `finding_1787271799_979271` showed the hole: a failed `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, MCP, UI, entity, or `Shutdown` would vanish from the latency data and only reduce throughput. With a throughput floor at `T` = 0.80, up to a fifth of operations could fail and the campaign could still pass, and a calibration run could bake its own failures into a low floor. That contradicts the ticket, which requires Hub operations to stay within every budget and not block.

Revision 5 closes it:

1. **Record every attempt at start.** For each of the nine operations plus terminal input/output, both arms, both phases, record the attempt when the operation starts after warm-up, then record the completion even if it finishes after the window. Report attempts, successes, failures, and incomplete cycles per operation. Counts are part of the campaign evidence, not a debug aid. A late successful completion is a counted attempt and a success.
2. **The measurement arms expect zero failures.** Calibration and acceptance run **no fault injection** — the eleven faults are separate lanes in section 12.5. The expected outcome of every attempted operation in a measurement arm is therefore success.
3. **Any other outcome is an immediate `product_failure`**, in calibration and in acceptance alike: an unexpected operation error, a request timeout, a client disconnect, an incomplete or truncated response, or a cycle that does not complete every step. The run stops; it is not downgraded to a lower score.
4. **Percentiles use successful samples only**, as before. That is now safe because a single failure has already failed the campaign, so no failure can silently shift a percentile by leaving the population.
5. **A failed calibration cannot set a threshold.** Calibration that records any failure is invalid under A.5 and produces no dataset and no thresholds. A depressed throughput floor derived from lost work is impossible by construction.

Typed results that a *fault lane* declares as expected — `shed_full`, `shed_busy`, `rejected_over_rate`, `event_gap`, and the rest of section 12.5 — are expected outcomes **in those lanes only**. They are never expected in a measurement arm, and they never appear on the nine measured operations, which the event plane must not be able to disturb at all. That is the whole claim of this ticket.

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

- **any attempted operation in a measurement arm fails**, per A.2.1: an unexpected error, request timeout, disconnect, incomplete response, or an incomplete cycle. This applies to calibration and acceptance equally;
- **any fixed value in A.2 differs between calibration and acceptance**, machine profile, fleet, operation schedule, event emission, terminal cadence, or measurement definition. Matching machine fields alone are not enough;
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
| `plane-enabled` | `N` lightweight sessions, `examples/event-plane-producer` emitting on the fixed schedule in section 5A.2 — 150 events per second in 25-event bursts with a 4 KiB payload, deliberately above `package_rate_per_sec` 100, `examples/event-plane-consumer` subscribed, one Unix and one WebRTC host-control client holding event subscriptions, and one attached noisy PTY carrying terminal input and output |
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

Three artifacts, all under `docs/reports/`:

- `<slug>-calibration.json`, the committed calibration dataset and derived immutable thresholds from section 5A.4 phase 1.
- `<slug>-implement.md`, the narrative report. It must keep the new event-plane coexistence regression budgets distinct from the pre-existing North Star behavioural contract, and must not present a fixture result as a product contract result.
- `<slug>-evidence.json`, the acceptance evidence. **Section 5G.6 is the single authority on which revisions it records**, split into executed revisions and cited prerequisite revisions. No other section states a competing rule.

### G. Client proof at public boundaries, with generic fixtures

Revisions 3 and 4 built a product-specific chain: the real `botster-project-pipelines` package installed on one caller-owned Hub, a bound Project Pipelines run, one shared `north-star-shared` session, and new product lanes in botster-web and botster-tui. The human rejected that chain after Plan Review had approved revision 5, and Plan Review `finding_1787278015_548510` records the rejection.

**The architectural reason is the one that matters, not the process history.** Botster components are libraries. A Hub load campaign that can only be proved by installing one first-party product package, binding one product run, and editing two client repositories is not proving a library boundary; it is proving one product configuration. Final integration proof must use repository-owned generic contract fixtures at public boundaries, and each repository must keep its own canonical contract proof.

#### G.1 What this campaign proves, and where

| Concern | Proved by | Where |
| --- | --- | --- |
| Package event admission, routing, fanout, shed, and bounded queues under saturation | a small fixture plugin driven through the **real** `HubPackageManifest` ABI and the **real** `PackageEventRouter` | this repository, `examples/event-plane-producer` and `examples/event-plane-consumer` |
| Causal-scope rejection under saturation | the existing cycle fixture | this repository, `examples/event-plane-cycle` |
| Full plugin ABI surface under saturation | the existing matrix package | this repository, `fixtures/plugins/plugin-contract-matrix` |
| Generic client event consumption over the host control protocol | a **new** generic event conformance entrypoint, built from the proven Hub-local helpers and driven under saturation. See 5G.1.1 | this repository, added to `crates/botster-hub-test-support/src/lib.rs` |
| Terminal behaviour under saturation | the existing many-PTY conformance shape and the two adapter suites | this repository, `run_many_pty_client_attach_conformance` (`:553`), `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`, `webrtc_terminal_adapter.rs` |

Every row is inside botster-hub, uses an existing repository-owned fixture, and needs no client checkout, no Node toolchain, and no product package.

#### G.1.1 The generic event conformance fixture does not exist yet, and this ticket adds it

Revision 7 named `botster_hub_test_support::run_client_conformance` (`crates/botster-hub-test-support/src/lib.rs:1440`) as the authoritative generic proof for client event consumption. Plan Review `finding_1787278903_138982` rejected that, and direct source inspection confirms the rejection: that function proves status, compatibility, sessions, spawn, terminal attach, input, resize, drain, and lifecycle, and contains **no** `SubscribeEvents`, `UnsubscribeEvents`, `PackageEvent`, or `EventGap` path. A repository-wide search finds `SubscribeEvents` and `EventGap` nowhere in that crate. I cited a fixture without reading its body, and it cannot prove the event-plane claim.

The generic event proofs that **do** exist are Hub-local, in `tests/hub_daemon_lifecycle/package_event_plane.rs`, and they already enter through the public client boundary via `botster_hub_client::connect_for_package_event_subscriptions`, `subscribe_events`, `next_event`, and `take_skipped_events`:

| Existing Hub-local proof | What it establishes |
| --- | --- |
| `isolated_hub_unix_client_receives_unsolicited_package_event` (`:328`) | exact delivery on the host-control path |
| `isolated_hub_unnegotiated_subscribe_events_is_typed_error` (`:372`) | negotiation is required |
| `isolated_hub_rejected_terminal_hello_still_subscribes_to_package_events` (`:407`) | terminal rejection does not remove the host event feature |
| `isolated_hub_reconnect_does_not_replay_package_events` (`:460`) | reconnect mints a fresh subscription with no replay |
| `isolated_hub_subject_and_audience_admission_return_typed_errors` (`:497`) | exact subject and audience admission |
| `isolated_hub_unix_write_stall_emits_one_event_gap_then_status_progresses` (`:531`) | a stalled consumer yields one `event_gap` and control progresses |

This ticket promotes that proven behaviour into one reusable, repository-owned entrypoint and drives it under saturation:

- **File:** `crates/botster-hub-test-support/src/lib.rs`, a new `run_client_event_conformance` beside `run_client_conformance`.
- **Coverage, all at the public client protocol boundary:** exact owner-plus-name subscribe, event receive, subject filtering, slow-consumer `event_gap`, reconnect without replay, unsubscribe, and continued control-response progress while events flow.
- **Driven under saturation** by the campaign lane, not only in isolation, because the claim is about behaviour while the plane is saturated.
- **Published-surface check:** `botster-hub-test-support` is a published crate and npm package. Adding a Rust entrypoint should not change conformance fixture bytes, but Implement must confirm that and, if fixture bytes do change, follow [[Hub test support capability cutovers use a new unpublished package version]] rather than mutating a published version.

Section 9 lists the changed file. Section 12.3 item 13 cites this entrypoint rather than `run_client_conformance`.

#### G.2 The public boundary is already documented

`docs/client-protocol.md:1374` states the contract this revision follows: an external client that needs a true live-hub integration test depends on the client protocol crate plus the test-support crate, **not on the full `botster-hub` library**, and pins the UI contract by tag rather than by a Hub commit. The campaign proves the Hub side of that boundary with generic fixtures. It does not reach across it.

#### G.3 The full prerequisite graph, including the two public-seam tickets

Revision 7 listed three prerequisites and stopped there. Plan Review `finding_1787278903_125669` found that the two client cleanups themselves depend on two further tickets that change **public contracts**, and that hiding them made the architecture scope look smaller than it is. The full graph:

| Ticket | Repository | target_id | Delivers | Depends on |
| --- | --- | --- | --- | --- |
| `ticket_1787278643_145174` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | a bounded **package-owned client notice reaction descriptor** in the canonical `@trybotster/ui-contract`, admitted on `HubPackageManifest` beside `events.emitted`, projected onto `DaemonPackage`, published through the generated daemon protocol and `@trybotster/hub-test-support` metadata, plus a Hub-owned fixture package exercising the public ABI | nothing |
| `ticket_1787278658_151737` | botster-project-pipelines | `tgt_a72ca1a83d504385b8648f71409119ab` | declares the `question.opened` notice reaction in its manifest and emits `payload.subject` as the active agent session uuid, replacing client-side workflow filtering with subject targeting | `ticket_1787278643_145174` (`dependency_1787278661_690676`) |
| `ticket_1787278327_274484` | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | removes Project Pipelines owner, event name, payload, and entity-family knowledge from generic Web production code; neutral contract fixtures at the public protocol boundary | both above (`dependency_1787278671_574148`, `dependency_1787278676_422577`) |
| `ticket_1787278327_199618` | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | the same cleanup, keeping botster-tui-kit policy-free | the same seam tickets |
| `ticket_1787267568_492780` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | bounded event-plane observability counters, four distinct timeout counters, a saturation-safe read path, four `BOTSTER_ENV=test` seams | nothing |

Two of these are **material public-contract changes**, and this plan names them rather than letting transitive engine edges hide them:

- The Hub descriptor ticket adds a new declaration to `HubPackageManifest`, a new projection on `DaemonPackage`, and new DTOs in the generated protocol and in published test-support metadata. That is a change to the very contract this campaign saturates.
- The Project Pipelines ticket changes an emitted event's payload by adding `payload.subject`.

**Correction to revisions 6 and 7.** Both said Project Pipelines is unchanged and out of scope. That is now false: `ticket_1787278658_151737` changes its manifest declaration and its emitted payload. The accurate statement is that **this campaign does not change Project Pipelines and does not execute it**, while a prerequisite in that repository does change it. Section 6.1, section 7, and section 14.1 are corrected to say exactly that.

**What the client cleanups consume.** Each consumes the merged `@trybotster/ui-contract` descriptor and its `DaemonPackage` projection from `ticket_1787278643_145174`, and the Web cleanup additionally consumes the subject-carrying `question.opened` from `ticket_1787278658_151737`. This campaign consumes none of those directly; it consumes the resulting **generic** client code as merged state.

**Effect on this campaign's own surface.** The descriptor ticket changes `HubPackageManifest` admission and `DaemonPackage`. Implement must confirm whether the saturation fixtures in `examples/event-plane-producer` and `examples/event-plane-consumer` need a notice-reaction declaration once that lands, and whether the new descriptor admission path belongs in the fault matrix as a malformed-declaration rejection lane. Recorded as unknown U7.

#### G.3.1 Canonical contract proof stays in each owner repository

This campaign cites these; it does not orchestrate, wire, or re-run them.

| Repository | Canonical proof it owns |
| --- | --- |
| botster-project-pipelines | that its emitted `question.opened` matches its published contract, including the new subject, per `ticket_1787278658_151737` and [[event plane client proof uses library contract fixtures]] |
| botster-web | its generic package-event client behaviour, proved by the neutral fixture from `ticket_1787278327_274484` |
| botster-tui | the same, from `ticket_1787278327_199618` |
| botster-core | terminal byte ownership and the plugin admission classes; unchanged |

#### G.4 The shared terminal-session identity requirement is removed

[[cross-client acceptance uses one live session identity]] governed revisions 3 and 4 and no longer governs this ticket. There is no `north-star-shared` session, no bound run, and no cross-client identity join in this campaign. The North Star behavioural oracles are still proved under saturation, on this repository's own attached noisy PTY, exactly as section 12.3 item 10 states. That is a Hub-owned terminal claim, not a cross-client product claim.

#### G.5 Consequences for the workflow

The loaded workflow stays **single-repository**, which is what it already is. No `web_sha`, `tui_sha`, or `project_pipelines_sha` input. No additional `actions/checkout`. No Node or npm setup. No cross-repository credential, which retires the operator precondition that revision 3 raised as unknown U6 and the risks that depended on it. `script/prove-north-star-shared-session` is untouched by this ticket.

#### G.6 Evidence shape: executed revisions versus cited prerequisite revisions

Plan Review `finding_1787278903_320205` found that sections 5F, 5G.3, 5G.6, 7, and 14.1 contradicted each other about which revisions the report records. They are now one rule, split by role:

**Executed revisions.** Exactly two, because the campaign runs only Hub code against its locked Core:

| Field | Value |
| --- | --- |
| `botster-hub` | the exact merged Hub revision the campaign ran |
| `botster-core` | the exact revision that Hub's `Cargo.lock` pinned for that run, recorded separately per [[live hub proof records distinct hub and locked core binary provenance]] |

**Cited prerequisite revisions.** Recorded so the boundary claim is checkable, and clearly labelled as **not executed by this workflow**:

| Field | Value |
| --- | --- |
| `botster-hub` descriptor and observability prerequisites | merged revisions of `ticket_1787278643_145174` and `ticket_1787267568_492780` |
| `botster-project-pipelines` | merged revision of `ticket_1787278658_151737`, plus its own gate artifact id for the contract proof it owns |
| `botster-web` | merged revision of `ticket_1787278327_274484`, plus its gate artifact id |
| `botster-tui` | merged revision of `ticket_1787278327_199618`, plus its gate artifact id |

The report must state plainly that the Hub workflow executes no client or package code, and that the cited revisions are the merged state that makes the boundary claim true rather than artifacts this campaign ran. That satisfies the ticket's requirement to use all five main revisions without pretending the campaign executed them.

The JSON keeps the flat-SHA `revisions` shape of `docs/reports/bounded-hub-resources-fresh-campaign-evidence.json` for both blocks, under `executed_revisions` and `cited_prerequisite_revisions`, plus the runner `provenance` block from `docs/reports/focused-ubuntu-idle-cpu-resource-bound-evidence.json`.

## 6. Repository ownership boundaries and cross-repo dependencies

### 6.1 Ownership

| Concern | Owner |
| --- | --- |
| Budget publication, campaign harness, verdict rules, evidence | botster-hub (this ticket) |
| Event-plane counters and their read path | botster-hub (dependency in 6.2) |
| Lifecycle journal, wake, pages, plugin admission classes | botster-core, unchanged |
| `question.opened` contract | botster-project-pipelines. **Changed by prerequisite `ticket_1787278658_151737`**, which adds the notice reaction declaration and `payload.subject`. This campaign neither changes nor executes it |
| Transient-event consumption | botster-web and botster-tui, **unchanged and not exercised by this campaign**. Each keeps its own canonical isolated-Hub lane. Generic client consumption is proved here at the public boundary per section 5G.1 |
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
- **This run edits and executes only botster-hub against its locked Core.** It changes no file in botster-core, botster-web, botster-tui, botster-tui-kit, or botster-project-pipelines, checks none of them out, installs none of them, and drives none of their harnesses. Client and package repositories **are** prerequisites as merged state (section 5G.3), and their merged revisions are cited but not executed (section 5G.6). Those two statements are scoped differently and do not conflict.
- No real `botster-project-pipelines` package is installed, and no bound product run, shared `north-star-shared` session, or cross-client identity join is created. Note the precise claim: a prerequisite in that repository **does** change it (`ticket_1787278658_151737`); this campaign neither changes nor executes it.
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

**A4 — client proof happens at public boundaries with generic fixtures, and the clients are made generic by prerequisite tickets rather than assumed generic.** Revisions 3 and 4 required the real Project Pipelines package, a bound product run, one shared session, and new product lanes in the clients. The human rejected that chain, and Plan Review `finding_1787278015_548510` records it. Revision 7 adds the missing half of the correction: both clients today hardcode the Project Pipelines owner, event name, payload, and entity families in production code, so a Hub-side generic proof would be representative of nothing shipped. `ticket_1787278327_274484` and `ticket_1787278327_199618` remove that coupling and deliver neutral contract fixtures at the public protocol boundary. This campaign consumes their merged state. It still checks out no client repository, installs no product package, and drives no client harness.

**A5 — the baseline arm is projection-decoupled, not event-disabled.** The ticket permits either. Section 5C explains why the decoupled arm is chosen.

**A6 — `N = 300`.** "Hundreds" is satisfied at 300. The existing ceiling is 32, in an `#[ignore]` local test (`tests/hub_daemon_lifecycle/sessions.rs:5670`). 300 is a tenfold increase over any exercised count.

### Unknowns for Plan Review and Implement to resolve

**U1 — can the runner sustain 300 concurrent PTY sessions?** Each session consumes file descriptors and a PTY pair. `probe_fd_limit` and `probe_pty_allocation` (`tests/hub_daemon_lifecycle/harness.rs:244`, `:258`) already exist to classify exhaustion. Implement must measure the runner's `ulimit -n` and PTY ceiling first and record them in the published profile. If 300 is not admissible, the outcome is a `host_exhaustion` verdict and an escalation, not a silent reduction to a number that fits.

**U2 — `DAEMON_MAX_CONNECTIONS` is 64.** Sessions are not connections, so 300 sessions are admissible. The campaign must keep its client count well below 64 and must not confuse the two limits. The existing 64-connection saturation case (`tests/hub_daemon_lifecycle/sessions.rs:3324`) stays a separate concern.

**U3 — does natural saturation reach lifecycle cursor expiry?** `DEFAULT_LIFECYCLE_JOURNAL_CAPACITY` is 1024. Whether 300 churning sessions can outrun the Hub cursor by more than 1024 changes is unmeasured. The dependency seam removes the uncertainty; Implement should still record whether natural expiry occurred.

**U4 — the reported `EventPlaneSnapshot` read path.** Section 6.2 item 6 requires a read path that does not contend with the router mutex. Implement must confirm the dependency delivered that property before trusting any saturation-time queue reading.

**U7 — the descriptor prerequisite may touch this campaign's own fixtures.** `ticket_1787278643_145174` changes `HubPackageManifest` admission and the `DaemonPackage` projection. Implement must confirm whether `examples/event-plane-producer` and `examples/event-plane-consumer` need a notice-reaction declaration once it lands, and whether malformed-descriptor rejection belongs in the section 5E fault matrix.

**U5 — an existing budget test is nominal.** `published_owner_turn_budgets_fail_if_observe_walks_every_session` (`tests/session_projection_owner_loop.rs:207`) contains only `const` assertions and one discarded comparison. Despite its name it cannot fail if observe walks every session. The campaign must not cite it as owner-turn evidence. Whether to repair or rename it is a separate decision recorded as a vault gap in section 13.

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
| `crates/botster-hub-test-support/src/lib.rs` | add `run_client_event_conformance`, the generic event conformance entrypoint from section 5G.1.1, built from the proven `package_event_plane.rs` helpers. Confirm whether conformance fixture bytes change; if they do, follow the unpublished-version rule |
| `tests/hub_daemon_lifecycle/mod.rs` | register the new module |
| `script/run-loaded-daemon-lifecycle` | one new `--test-target event-plane-saturation` case beside the existing map at `:978-1024`. No downstream-leg surface is added here; that stays a sibling step |
| `.github/workflows/loaded-daemon-lifecycle.yml` | **one line**: a new `options:` entry in the `test_target` choice list at `:14-30`. No new inputs, no new checkouts, no toolchain change. The workflow stays single-repository |
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
| R12 | The campaign is read as proving a product configuration rather than a library boundary | Section 5G names one generic fixture per claim and cites each owner repository's canonical proof. The report must not present a fixture result as a product contract result |
| R14 | The campaign asserts sibling survival on the fail-closed path and fails against correct code | Section 11.6 states the shipped policy and explicitly forbids that assertion. The campaign asserts the bounded blast radius instead |
| R19 | The campaign runs against client revisions that predate the cleanups, so the boundary claim is asserted while the shipped clients are still product-coupled | Section 14.1 makes both cleanup tickets prerequisites that merge before Implement. The evidence must record that the campaign ran after both merged, not merely that they exist |
| R18 | A failed Hub operation disappears into the throughput floor | Section 5A.2.1 records every attempt and makes any failure in a measurement arm an immediate product_failure in both phases, so no failure can be absorbed by the `T` = 0.80 tolerance and no calibration can bake its own losses into a low floor |

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
13. **Generic client consumption at the public boundary**, through the new `run_client_event_conformance` from section 5G.1.1 and **not** `run_client_conformance`, which has no event path: exact owner-plus-name subscribe, event receive, subject filtering, slow-consumer `event_gap`, reconnect without replay, unsubscribe, and continued control-response progress, all under saturation. No product package and no shared product session take part.

### 12.4 Published budget gates

For each of `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, `Shutdown`, MCP, UI, entity, terminal input, and terminal output, in both arms and in both phases:

- record p50, p95, p99, maximum, and throughput under the fixed nearest-rank method in section 5A.2;
- **gate every one of the five metrics**, absolute and relative, exactly as section 5A.3 defines. p50, p95, p99, and maximum gate as ceilings; throughput gates as a floor at `T` = 0.80. None of the five is recorded-only;
- **record attempts, successes, and failures per operation**, per section 5A.2.1. Any failure in a measurement arm is an immediate `product_failure` in both phases;
- record queue count and bytes, oldest age, admission latency, delivery latency, shed by typed reason, gap count, resync count, pressure, **the four timeout counters T1 through T4 from section 4.4.1, each a distinct counter**, owner-turn duration, and ready-operation wait.

The recorded-signal list requires dependency `ticket_1787267568_492780`. Without it these signals have no read path and the ticket cannot pass.

Every failure rule in section 5A.5 applies. A failed operation attempt, a profile or workload mismatch, a missing metric, an invalid calibration, or any threshold breach on any of the five metrics fails the campaign. None of these is a caveat, and none is absorbed by the throughput floor.

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
  -f test_target=event-plane-saturation \
  -F repetitions=<published> \
  -f stress_profile=none
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
8. **The loaded workflow is structurally single-repository**, and under the library boundary that is a feature rather than a gap. It has two `actions/checkout` steps, both Hub, and no Node or npm. Capture that a Hub campaign proving a library boundary needs none of that, and that a plan wanting cross-repository product proof is asking the wrong workflow.

## 14. Pipeline gates and artifacts

| Item | Value |
| --- | --- |
| Gate | `botster_stack_plan_gate` |
| Plan artifact | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md`, revision 10 |
| Vault checklist | `checklist_1787266824_449406` |
| Plan Reviews answered | `review_1787268374_271226` (6), `review_1787270029_776949` (4), `review_1787271188_552110` (3), `review_1787271799_830342` (1), `review_1787278015_433684` (1 blocker, superseding approval `review_1787272071_523159`), and `review_1787278903_443047` (3) |
| Human answers folded in | `question_1787267931_572353` (routing exception), `question_1787268530_910910` (budget nature and derivation), and the library-boundary decision recorded in `review_1787278015_433684` |
| Delivery | direct merge into `main`; no pull request; no human pull-request sign-off |

### 14.1 Five prerequisites, in dependency order

Section 5G.3 carries the delivery detail and the reason each is required. This is the ordering and edge handling.

| Order | Ticket | Repository | target_id | Blocked by |
| --- | --- | --- | --- | --- |
| 1 | `ticket_1787278643_145174` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | nothing |
| 1 | `ticket_1787267568_492780` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | nothing |
| 2 | `ticket_1787278658_151737` | botster-project-pipelines | `tgt_a72ca1a83d504385b8648f71409119ab` | `dependency_1787278661_690676` |
| 3 | `ticket_1787278327_274484` | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | `dependency_1787278671_574148`, `dependency_1787278676_422577` |
| 3 | `ticket_1787278327_199618` | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | the same two seam tickets |

The two order-1 tickets are independent of each other and of everything else. The engine edges above already enforce the rest, so ordering is enforced rather than described.

Each is registered against its own repository target, per [[cross repo dependency registration must use dependency repo target]]. Two of the five change public contracts, named explicitly in section 5G.3 rather than left implicit.

**Closed as superseded, and not to be restored:** `ticket_1787271303_548807`, `ticket_1787270342_754581`, and `ticket_1787270386_991884`. Those added product coupling; the current set removes it. [[event plane client proof uses library contract fixtures]] records that closure and its reason.

**Edge handling.** `ticket_1786663585_879846` has no open dependency edge, which is what lets this review-only Plan return advance, per human answer `question_1787267931_572353`. After this revision passes Plan Review and **before any Implement advance**, add all five edges above with `project_pipelines_add_ticket_dependency`. Let them merge in the order the engine enforces. Then start Implement here.

**Evidence obligation.** Section 5G.6 is the single authority on recorded revisions. The report records two **executed** revisions, Hub and its locked Core, and cites the merged revisions plus gate artifact ids of the five prerequisites as state this campaign did not execute. Risk R19 covers running before they merge.
