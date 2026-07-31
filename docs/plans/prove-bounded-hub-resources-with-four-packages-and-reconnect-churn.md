# Prove bounded Hub resources with four packages and reconnect churn

## Target and planning context

- Ticket: `ticket_1785199716_875648` — Integration: prove bounded Hub resources with four packages and reconnect churn.
- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline run/step: `run_1785512302_815848` / `run_step_1785512302_232218` (`botster_stack_plan`).
- Repository playbook: [[botster-hub-playbook]].
- Role and workflow playbooks, loaded in the required order: [[planner-playbook]], [[botster-planner-playbook]], [[botster-hub-playbook]], targeted notes and surface playbooks listed below, then [[project-pipelines-playbook]].
- Surface playbooks: [[botster-runtime-reviewer-playbook]], [[botster-package-reviewer-playbook]].
- Architecture maps: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Project Pipelines context inspected: ticket, run, current step and gate, reviews, findings, artifacts, dependencies, questions and answers, and sibling tickets on the resolved target.

The routed ticket worktree, not an ambient or legacy checkout, is authoritative. Before planning, `origin` was fetched and `origin/main` was fast-forward merged. Exact base evidence:

- required merge commit: `822a75af9c9cc1815a2aaff18f3294d82810fd1f` (PR #184);
- planning HEAD: `822a75af9c9cc1815a2aaff18f3294d82810fd1f`;
- `origin/main`: `822a75af9c9cc1815a2aaff18f3294d82810fd1f`;
- `git merge-base --is-ancestor 822a75af9c9cc1815a2aaff18f3294d82810fd1f HEAD`: exit 0;
- `git rev-list --left-right --count HEAD...origin/main`: `0 0`;
- worktree was clean after the fast-forward.

## Vault context loaded

The plan is constrained by [[identity]], [[goals]], and these exact atomic notes:

- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[botster pipeline needs continuous product owner between agent steps]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[may supervise permits the hub to supervise the package entrypoint]]
- [[hub supervision admission changes require exact live hub launch proof]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[webrtc bootstrap origin must be requested after the package server binds]]
- [[plugin worker queue capacity and executor concurrency are independent host profile knobs]]
- [[durable state version preflight must precede shape deserialization after cold turkey changes]]
- [[plugin hardening needs lifecycle resource and observability layers]]
- [[daemon socket attach must detach subscriptions on disconnect and exit]]
- [[botster terminal clients share one sessionio data plane subscription path]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[botster session worker requires explicit build in dogfood launchers]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[load diagnostics must not cost work proportional to what they measure]]
- [[sid scoped census is blind to setsid session leaks]]
- [[subprocess harnesses must kill child on failed readiness]]
- [[botster plugins reload through mcp not file watching]]
- [[plugin timer callbacks run in plugin worker vms]]
- [[worker scheduled plugin timers fire locally instead of reentering worker dispatch]]

No convention conflict was found. The central constraints are that Hub owns host policy, lifecycle, admission, and sanitized debug projection; Core owns policy-free worker queue/executor enforcement and authoritative worker counters; terminal bytes remain on the SessionIo/ClientWorker data plane; queue capacity and executor concurrency stay separate; live proof records both Hub and locked Core worker provenance; and deterministic state/counter invariants, not timing alone, decide success.

## Repository evidence and current production path

The plan was built from `README.md`, `docs/client-protocol.md`, `docs/adr/local-runtime-production-readiness.md`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/loaded-daemon-lifecycle.yml`, the production and loaded lifecycle scripts, `src/daemon_transport.rs`, `src/client_api.rs`, `src/runtime.rs`, `src/capabilities.rs`, `src/config.rs`, `src/main.rs`, the Hub client and test-support crates, and the focused daemon, capability, and plugin lifecycle tests.

Important existing behavior to preserve and reuse:

- `script/test-production-package-runtime` already installs and enables exactly `botster-web`, `botster-tui`, `botster-workspaces`, and `project-pipelines` at exact source coordinates in a clean data directory, exercises public package/application/daemon paths, checks Web and TUI downstream behavior, runs upgrade coverage, and emits redacted evidence. The resource proof belongs in this path rather than a parallel installer.
- `DaemonStatus.lifecycle_counters` already exposes connection, subscription, reconnect, cleanup, reconciliation, delivery, and stalled-write counters. `PluginLifecycleStatus` already carries the Core-authored worker snapshot: configured queue capacity/concurrency, live executors/workers, queued jobs, and in-flight jobs.
- `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible` already proves one authoritative reconnect baseline, low event-driven idle wakeups, subscription cleanup, repeated reconnects, admission, malformed/half-open clients, and shutdown. The integration campaign should consume the same invariants and only add the missing four-package/runtime axes.
- `.github/workflows/loaded-daemon-lifecycle.yml` and `script/run-loaded-daemon-lifecycle` already provide exact-SHA, locked-toolchain, distinct Hub/Core-binary, repeated loaded-campaign proof and run-token/process-group/zombie cleanup census. This ticket adds one focused resource-bound selector to that machinery and reuses its cleanup census; it must not create a parallel CI runner or census.
- `hub_runtime_passes_split_plugin_worker_config_to_core_engine` in `tests/hub_plugin_lifecycle_test.rs` already proves distinct knobs (`queue_capacity = 7`, `executor_concurrency = 3`), three rather than seven live OS-backed executor workers, and unload retirement to the pre-load snapshot. The new proof preserves and consumes this mechanism instead of rebuilding it.
- `hub_runtime_schedules_cancels_and_cleans_up_timers` and `unload_cleans_up_capability_resources_for_plugin` in `tests/hub_capability_runtime_test.rs` already prove timer registration/firing/cancellation and owner-scoped unload cleanup with no post-cleanup firing. Production cleanup is `HubCapabilityRuntime::cleanup_plugin` in `src/capabilities.rs`, reached from `HubRuntime::unload_plugin_package` in `src/runtime.rs`. Reload-path cleanup is the specific missing timer fixture.

The four installed package sources were inspected at `botster-web@9d1607…`, `botster-tui@c426b8…`, `botster-workspaces@c78f3b…`, and `project-pipelines@f2266a…`. Their manifests, entrypoints, and actions declare no plugin timer registration. `botster-tui-kit` remains the production script's exact source-revision build/test input; it is not installed or enabled and therefore is not a fifth package in the runtime shape.

## Binding clarification and assumptions

Question `question_1785512651_762093` was answered by the product orchestrator. The binding interpretation is:

- keep the production workload at exactly the four named packages;
- prove from those package sources that no timer is declared or registered;
- require Hub timer resource/debug counters to remain at their documented zero baseline through startup, real package actions, reconnect churn, explicit plugin reload, idle settling, and shutdown;
- do not impersonate a package owner, add a synthetic fifth package, or create an artificial cross-repository timer dependency;
- cite or strengthen a separate Hub-owned deterministic timer fixture for the nonzero registration/firing/cancel/reload/cleanup mechanism.

Assumptions that implementation must validate rather than silently preserve:

- All four resolved package revisions remain installable through the public package commands and expose the actions used by the campaign. Revision drift must fail before launch rather than silently substitute another checkout.
- A minimal sanitized timer-resource count can be projected through the existing daemon lifecycle response without exposing plugin internals or adding a new service/endpoint. If the existing response can already derive it, no new DTO field is needed.
- Web can attach to the campaign-owned data directory and TUI can attach to the campaign-owned socket through their existing public live-test inputs. If a downstream harness cannot attach without owning another Hub, that is a dependency on that repository, not permission to edit it in this run.
- Package reload applies only to loaded plugin entrypoints; the campaign records which of the four packages have reloadable plugin owners and does not pretend client-only activity is a plugin reload.
- The CI regression uses the production Core defaults explicitly: configured queue capacity `256`, configured executor concurrency `2`, four loaded plugin owners, exactly eight live executor workers, and an absolute Hub-process ceiling of 64 OS threads. The ceiling is deliberately conservative across the fixed runtime overhead while still failing the reported pre-fix shape of about 1,041 threads; it is a committed invariant, not a value discovered from the observation under test.
- The no-stress Linux CI selector also enforces the initial idle CPU ceiling: no more than 250 ms of Hub process CPU-time growth during a five-second observation after counter convergence (5% of one core). macOS/Linux operator evidence applies the equivalent 5% ceiling through the documented platform recipe. Any threshold revision requires captured runner evidence and plan/review visibility rather than runtime calibration.

Unknowns to close while implementing:

- The exact reload path through which the existing unload timer cleanup should be exercised; review confirmed reload cleanup itself is not yet proven.
- Whether the current generated Hub client bindings include a lifecycle response shape suitable for the minimal timer-resource field or require regeneration.
- Which existing package smoke entrypoints can attach to one caller-owned Hub without modification. Any missing downstream capability must be registered as a dependency against that package repository.
- Whether the existing cleanup census functions can be sourced directly or need a small semantics-preserving extraction for reuse by the production campaign.

## Scope

Implement one production-path campaign that installs exactly the four packages into one clean data directory and proves, against that same live Hub and locked Core worker binary:

1. Exact package coordinates, declared entrypoints/actions, and a source-derived zero-timer declaration baseline.
2. Real package activity through public application/plugin/MCP paths, including Web and TUI attaching to the campaign Hub rather than substituting isolated test daemons.
3. Repeated normal unsubscribe/detach, abrupt EOF, reconnect, slow-consumer, plugin reload, public package disable/unload retirement, idle-settle, and shutdown generations.
4. Deterministic convergence of connection, entity, terminal-attach, worker, queue, timer, and cleanup counters after every generation.
5. Explicit high-water and current-resource bounds at production defaults (`256` queue capacity and `2` executor concurrency), including exactly eight executor workers for four loaded owners, the relative formulas at every snapshot, and an absolute ceiling of 64 Hub OS threads.
6. OS corroboration for threads, processes/process groups, sockets, and idle CPU on macOS and Linux, with exact Hub and Core worker binary provenance.
7. Redacted, bounded evidence suitable for local use and CI artifact retention.

## Non-scope

- No changes to `botster-core` queue/executor behavior or counter semantics.
- No changes to package source repositories from this Hub-targeted run.
- No fifth or synthetic package and no artificial timer behavior.
- No new Hub service layer, resource manager abstraction, generic metrics system, or broad CLI redesign.
- No terminal byte routing through Hub control-plane APIs.
- No replacement for the existing focused connection lifecycle or loaded-daemon campaigns.
- No timing-only sleep-and-inspect conformance gate; bounded waits may observe eventual convergence, but success is a state/counter invariant.
- No adjacent cleanup or wholesale retrofit of older tests.

## Ownership boundaries and dependencies

`botster-hub` owns this implementation: production campaign orchestration, host-level lifecycle/resource admission assertions, the sanitized daemon debug projection, capability/timer bookkeeping visibility, exact binary provenance, redacted evidence, and Hub-focused tests/docs.

`botster-core` remains authoritative for plugin worker queue capacity, executor concurrency, live executor/worker counts, queued jobs, and in-flight jobs. Hub must read the existing Core snapshot and must not recreate counters by polling or introspection. The closed Core queue/executor dependency and the closed Hub lifecycle/configuration/cleanup dependencies are prerequisites already satisfied in Project Pipelines.

`botster-web`, `botster-tui`, `botster-workspaces`, and `project-pipelines` are exact-coordinate acceptance inputs. The Hub campaign may invoke their existing public live/smoke entrypoints and inspect their manifests; it may not edit them. If one cannot attach to the caller-owned Hub or expose its declared production action, add a ticket dependency against that repository and stop that slice instead of broadening this run.

No new cross-repository dependency is currently planned. The sibling target ticket concerning BindList descendant identity is unrelated and is not absorbed here.

## Surgical implementation plan

### 1. Expose only the missing live resource observation

Thread a sanitized active timer-resource count from `HubCapabilityRuntime` through the existing runtime/client API and `PluginLifecycleStatus` response. Keep Core worker counters sourced from the existing live Core snapshot. Do not add a general resource ledger, new endpoint, or serialized configuration echo. Update generated Hub client bindings only if the public response schema changes.

Document the zero baseline: with the four exact package revisions loaded, active timer resources must be zero before activity and after every campaign phase. Preserve the separate Hub test fixture for the nonzero timer mechanism.

### 2. Add a caller-owned-Hub resource probe

Add a small standard-library script under `script/` that connects through the public daemon protocol to an already-running campaign Hub. It should fail with phase-labelled diagnostics and bounded redacted output. At each phase it captures one coherent product snapshot and asserts:

- current connections, entity subscriptions, and terminal attach subscriptions return to the phase baseline;
- reconnect registration and cleanup counters increase by the expected generation count, with no cleanup failures;
- only the initial/reconnect authoritative baselines appear and idle delivery/reconciliation deltas remain within the existing focused-test bound;
- queued and in-flight plugin jobs return to zero;
- live plugin executors equal the loaded reloadable-plugin owner count;
- live executor workers are no greater than `loaded_plugin_owners * configured_executor_concurrency`;
- no bound is multiplied by configured queue capacity, and queue depth never exceeds configured capacity;
- the production-default four-owner phase reports queue capacity `256`, executor concurrency `2`, exactly eight live executor workers, and no more than 64 total Hub-process OS threads;
- active timer resources stay exactly zero for the four-package workload;
- slow consumers increment bounded overflow/failure evidence without retaining a subscription or creating an unbounded producer;
- evidence collection itself is constant-cost and does not enumerate an unbounded historical event stream.

Use explicit convergence loops over authoritative counters with deadlines and immediate child cleanup on readiness or assertion failure. A timeout reports the last snapshot; it is not itself the success criterion.

### 3. Extend the existing exact four-package production campaign

In the fresh-mode path of `script/test-production-package-runtime`:

- verify all existing source inputs, including `botster-tui-kit`, at their exact revisions; record manifest/entrypoint/action/timer-declaration facts for the four installed packages while retaining TUI Kit as a build/test input rather than installing it;
- start one Hub from the freshly installed coordinates and record the Hub executable realpath/SHA plus the locked Core worker executable realpath/SHA;
- run actual Project Pipelines and Workspaces plugin actions through their public Hub surfaces;
- run Web against the same data directory and TUI headless against the same socket using their existing caller-owned-Hub inputs;
- create a worker-backed session and exercise entity plus terminal subscriptions across normal detach, abrupt client loss, rapid reconnect, and slow-consumer phases;
- explicitly reload the packages with reloadable plugin owners through the public package reload command, never by touching files;
- after the four-enabled-package churn/reload/idle proof, disable each reloadable package through the public package disable path and assert live plugin executors/workers retire stepwise to the pre-load baseline before `down`;
- invoke the resource probe before activity, after each churn/reload generation, after idle settling, and before/after orderly down;
- after `down`, reuse the loaded lifecycle cleanup census across executable provenance and relevant process groups—not one SID—to gate on zero surviving Hub processes, zero owned session workers, zero zombie children, and zero live or stale sockets under the campaign data directory;
- retain the existing upgrade leg and ensure it cannot mask the fresh resource proof.

The production evidence helper should add bounded JSON snapshots for declared package facts, lifecycle counters, worker/timer resource counters, OS census, and phase results while preserving existing secret/path/PII redaction.

### 4. Add the CI-executable focused regression and close only mechanism gaps

Add one exact focused Rust test, `focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload`, under `tests/hub_daemon_lifecycle_test.rs`. It starts a real Hub with four deterministic loaded plugin owners at the explicit production defaults, drives reconnect/reload/idle/disable generations, and enforces the same committed resource constants as the production campaign:

- `configured_queue_capacity == 256` and `configured_executor_concurrency == 2`;
- `live_plugin_executors == 4` and `live_executor_workers == 8` while all four owners are loaded;
- Hub-process OS thread count is at most 64, so the reported approximately 1,041-thread failure cannot pass;
- queue/in-flight work, connection/subscription state, and timer resources converge to baseline;
- disable/unload retires live executors and executor workers to the pre-load snapshot;
- on Linux the test always records the post-convergence five-second CPU-time
  delta; it asserts the 250 ms ceiling only when
  `BOTSTER_ASSERT_IDLE_CPU_BOUND=1` is set by the no-stress focused selector.

The deterministic four-owner fixture is the CI mechanism regression, not a substitute for the operator-run exact-package campaign and not a claim that the real packages register timers. Because it is a normal Rust integration test, `./test.sh` runs it. Add a `focused-plugin-resource-bounds` `test_target` to `.github/workflows/loaded-daemon-lifecycle.yml`, map it to the exact test in `script/run-loaded-daemon-lifecycle`, and require `stress_profile=none` for this selector. This is the named CI-executable threshold carrier; the cross-repository production campaign remains operator-run downstream proof.

Extend other focused tests only where the product observation or lifecycle mechanism is missing:

- verify the timer-resource field is live, sanitized, and zero for no-timer owners;
- preserve `hub_runtime_passes_split_plugin_worker_config_to_core_engine` as the distinct-knob/unload-retirement unit proof;
- preserve `hub_runtime_schedules_cancels_and_cleans_up_timers` and `unload_cleans_up_capability_resources_for_plugin` as the timer mechanism proof;
- add one focused Hub-owned reload-path test that registers a real timer under a test plugin owner, reloads that owner through the runtime lifecycle, and proves zero retained pre-reload timer resources and no post-cleanup firing;
- reuse `focused_connection_lifecycle_is_bounded_event_driven_and_counter_visible` for connection/subscription semantics rather than duplicating it in another large Rust test.

### 5. Document CI and local macOS/Linux proof

Add a short resource-proof document and link it from the runtime acceptance documentation/README. It must list exact commands, expected invariants, and artifact locations for:

- local macOS process/thread/socket census using universal `ps`/`lsof` fields;
- local Linux census using `ps` plus `/proc` where available;
- the exact four-package fresh campaign;
- the new `focused-plugin-resource-bounds` selector with `stress_profile=none`, the existing focused connection selector, and the loaded daemon workflow.

CPU is corroborating evidence, not the sole idle gate. The no-stress focused Linux selector gates on no more than 250 ms of Hub CPU-time growth over five seconds after deterministic convergence, and the operator macOS/Linux recipe gates on the equivalent average of no more than 5% of one core plus no growth proportional to connection/subscription count. Other loaded/stressed jobs record CPU but gate on resource/counter convergence instead.

## Expected affected surfaces/files

- `src/capabilities.rs` — authoritative active timer-resource observation.
- `src/runtime.rs`, `src/client_api.rs`, `src/daemon_transport.rs` — existing-path propagation of the sanitized observation, only as required.
- `crates/botster-hub-client/src/lib.rs` and generated TypeScript client artifacts — response schema update only if the DTO changes.
- `crates/botster-hub-test-support/src/lib.rs` — shared snapshot/probe support only when it removes duplication from focused tests.
- `tests/hub_capability_runtime_test.rs` and/or `tests/hub_plugin_lifecycle_test.rs` — zero baseline and any missing reload timer cleanup proof.
- `tests/hub_daemon_lifecycle_test.rs` — the exact CI-focused four-owner/default-knob/absolute-thread-bound regression plus minimal public-response assertions; retain the existing focused connection campaign.
- `script/test-production-package-runtime` — wire resource phases into the real four-package path.
- `script/production-package-runtime-evidence` — bounded, redacted resource/provenance artifacts.
- One focused new `script/` resource probe and its script-level self-test if needed; reuse or minimally extract the loaded lifecycle cleanup census instead of implementing a second process/zombie census.
- `README.md` and a focused `docs/` resource-proof guide.
- `.github/workflows/loaded-daemon-lifecycle.yml` and `script/run-loaded-daemon-lifecycle` — add the `focused-plugin-resource-bounds` no-stress CI selector and reuse its existing survivor/zombie cleanup machinery.

This list is a forecast, not permission to touch every file. Each changed line must serve the missing live observation, production-path wiring, deterministic proof, or documentation made necessary by those changes.

## Risks and mitigations

- **False proof from another Hub:** require one caller-owned data directory/socket and record PID plus executable provenance for every downstream phase.
- **Reimplementing Core state in Hub:** consume `DaemonPluginWorkerCounters`; never infer executor bounds from config or process counts.
- **Flaky timing thresholds:** gate on authoritative convergence and explicit high-water/current bounds; keep CPU observational except for a conservative unstressed ceiling.
- **Ceiling accidentally ceases to cover production defaults:** the focused and exact-package campaigns both assert `256`/`2`, eight workers, and the fixed 64-thread ceiling before accepting resource evidence.
- **Process census blind to `setsid`:** census the relevant process tree/process groups system-wide by executable provenance, not only one SID.
- **Slow-consumer deadlock or leaked child:** use bounded writes/readiness and unconditional child teardown on every failure path.
- **Probe-caused load:** take fixed-size snapshots and avoid loops proportional to queue capacity, event history, or subscriber count.
- **Schema churn:** add only the timer count missing from the existing lifecycle response and regenerate checked-in clients atomically.
- **Package behavior drift:** fail on coordinate/manifest mismatch before launch and store the resolved coordinates in evidence.
- **Secret/PII leakage:** pass every new evidence field through the existing redaction and negative-scan checks.
- **Manufactured timer coverage:** keep zero-timer production proof and nonzero Hub-owned timer fixture explicitly separate.

## Acceptance checks and downstream proof

Implementation is complete only when all applicable checks pass from a clean routed worktree:

1. `cargo fmt --check` and `cargo clippy --all-targets --all-features -- -D warnings`.
2. `./test.sh` for the Hub workspace.
3. `./test.sh --test hub_daemon_lifecycle_test focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload -- --exact --nocapture` proves in a normal CI-runnable Rust test that production defaults are `256`/`2`, four owners create exactly eight executor workers, the Hub stays at or below 64 OS threads, all resource counters converge, and public disable/unload retires workers to baseline. On Linux it records the five-second CPU delta but asserts the 250 ms ceiling only when `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`; `script/run-loaded-daemon-lifecycle` sets that signal exclusively for `focused-plugin-resource-bounds` and rejects any stress profile other than `none`.
4. The existing focused connection lifecycle selector continues to prove event-driven connections/subscriptions and reconnect cleanup.
5. Focused capability/plugin lifecycle tests prove zero timer observation plus `hub_runtime_schedules_cancels_and_cleans_up_timers`, `unload_cleans_up_capability_resources_for_plugin`, the new reload cleanup path, and no post-cleanup firing; `hub_runtime_passes_split_plugin_worker_config_to_core_engine` remains the distinct-knob/unload-retirement unit regression.
6. Script self-tests cover readiness failure, timeout diagnostics, redaction, child teardown, counter non-convergence, and supported macOS/Linux census parsing.
7. The exact-coordinate fresh production campaign retains all existing exact source inputs, installs only the four named packages, drives their real declared actions, attaches Web and TUI to the same Hub, performs reconnect/slow-consumer/reload/idle/public-disable/down phases, and emits all resource/provenance artifacts.
8. Every loaded phase runs at configured defaults `256`/`2` and satisfies both absolute and relative bounds: exactly eight executor workers for four owners; no more than 64 Hub OS threads; no retained connection/entity/attach/timer resources; empty queue/in-flight work; workers bounded by owners times concurrency; queue depth bounded independently by capacity; no cleanup failure; no growth with reconnect generation count.
9. After public disable, live plugin executors/workers equal the pre-load baseline. After orderly `down`, the shared provenance/process-group census asserts zero surviving Hub processes, zero owned `botster-session-worker` processes, zero zombie children, and zero live or stale sockets under the campaign data directory. A SID-only result cannot satisfy this gate.
10. The unstressed idle observation has stable deterministic counters and CPU within the committed near-zero ceiling on the CI Linux selector and both supported operator recipes; stressed runs use deterministic resource gates and record CPU only.
11. `.github/workflows/loaded-daemon-lifecycle.yml` exposes `focused-plugin-resource-bounds`, runs it with `stress_profile=none`, and records exact Hub plus locked Core worker provenance and the existing survivor/zombie evidence. The existing `focused-connection-lifecycle` selector remains complementary downstream proof.
12. `script/production-package-runtime-evidence verify` and the repository's secret/PII scans accept the new bounded artifacts.
13. A final source scan confirms the four installed package revisions still declare zero timers; evidence shows the live timer count stayed zero rather than merely omitting the check. `botster-tui-kit` remains an exact source/build input and is not counted as an installed package.

Code existence is insufficient: evidence must show the production `script/test-production-package-runtime` entrypoint invoked the probe against the same Hub used by all four installed packages and downstream Web/TUI checks.

## Project Pipelines and vault checklist evidence

- Target resolution, base synchronization, repository/CI inspection, and the product-owner clarification are durable checklist items for this run.
- Vault checklist evidence cites the exact resolvable wikilinks above, records no convention conflict, records plan verification commands, and records whether durable knowledge was captured.
- The two checklist creation calls returned after the MCP timeout boundary, but listing the run checklists confirmed both durable records existed; no duplicate was created.
- This document is the repository-visible plan artifact. Gate evidence must reference it, the exact target/target ID, the binding question answer, base synchronization, affected surfaces, risks, and acceptance checks before requesting step advancement.

## Vault gaps worth capturing

No new durable methodology note is required during planning: the existing notes already specify the ownership, bounded-resource, event-driven, process-census, timer, provenance, and deterministic-conformance constraints. After implementation, capture a new atomic note only if the campaign establishes a reusable cross-platform resource-bound formula or a previously undocumented limitation in caller-owned-Hub package smoke tests. Do not capture ticket-specific commands or transient thresholds as general vault knowledge.
