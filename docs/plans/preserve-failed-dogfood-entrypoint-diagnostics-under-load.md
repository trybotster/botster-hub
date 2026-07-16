# Preserve failed dogfood entrypoint diagnostics under load

## Context loaded

- Project Pipelines context for ticket `ticket_1784168176_753693`, run
  `run_1784222795_280840`, active Plan step `botster_plan`, run step
  `run_step_1784222796_613436`, and gate `botster_plan_gate`. There are no prior
  artifacts, findings, reviews, questions, answers, or blocking dependencies.
- Required planning context: [[identity]], [[goals]], [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], and
  [[spa-patterns]].
- Required Botster overlay notes: [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]], and
  [[plan agents must author vault context as wikilinks not home paths]].
- Ticket-specific architecture and testing constraints: [[botster runnable entrypoints are hub owned launch contracts]],
  [[installed apps are daemon app rows projected from package runnable entrypoints]],
  [[structured output fields need producer paths or explicit scaffold disposition]],
  [[botster hub diagnostics use daemon diagnostic rows in client dtos]],
  [[retention without a reachable flush is data loss]],
  [[lifecycle guards evaluated before the reconciling drain are one call stale]],
  [[test script required for rust tests not cargo test]],
  [[botster test sh forwards arguments to cargo not custom unit flags]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[a poisoned test lock is a symptom not a waiver]],
  [[suite wide acceptance criteria make every observed test failure in scope]],
  [[full suite hangs need source and behavior proof before unrelated waivers]],
  [[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]],
  and [[plan steps need reviewable plan artifacts]].
- Repository path traced:
  `botster-hub dogfood` starts `botster-web/web-client` in `src/main.rs`, polls
  `DaemonRequest::PackageEntrypointStatus`, the daemon projects
  `EntrypointSupervisor::snapshots()` through `src/daemon_transport.rs`, and
  `failed_web_entrypoint_status` renders the process state, exit status, and up
  to four diagnostics into the operator error. The acceptance test at
  `tests/hub_daemon_lifecycle_test.rs:5271` drives this real binary/socket path
  with a child that writes `bridge bind failed: fixture` to stderr and exits 42.
- Root-cause evidence from `src/entrypoint_supervisor.rs`: `refresh` drains the
  detached reader channels before `Child::try_wait`. It can then observe exit 42,
  publish `failed`, and return a snapshot while the reader threads still own the
  final stdout/stderr buffers. Under load, the dogfood readiness loop sees the
  terminal state and exits before a later status request can project stderr.
- Existing loaded verification surface: `.github/workflows/loaded-daemon-lifecycle.yml`
  and `script/run-loaded-daemon-lifecycle` precompile the exact lifecycle test,
  run the full test binary at default Cargo parallelism under the bounded
  `residual-tail` CPU profile, preserve the first red run, and retain resource
  and teardown evidence.
- Planning probe: the exact dogfood test initially failed because the focused
  invocation had not materialized `target/debug/botster-session-worker`. After
  `BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker`,
  `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics -- --exact --nocapture`
  passed once. This isolated green is only a baseline; it does not disprove the
  captured scheduler-dependent race.

## Scope

### In scope

- Make failed/exited supervised-entrypoint snapshots causally complete: a
  terminal process state must not become externally visible before the bounded
  stdout/stderr readers have delivered all captured bytes associated with that
  exit.
- Preserve the existing 4096-byte per-stream bound, path redaction, diagnostic
  kinds, exact exit status, and public package/app projection shapes.
- Add a deterministic supervisor regression that controls delayed reader
  completion and proves a failed snapshot cannot outrun its stderr diagnostic.
- Keep the existing real `botster-hub dogfood` assertion exact and prove the
  production CLI/socket/projection path still renders
  `stderr: bridge bind failed: fixture` with `exit:42`.
- Prove the regression test goes red with only the production fix reverted.
- Verify the complete lifecycle test target at default parallelism under the
  existing isolated residual-tail campaign; stop and retain the first red run.

### Non-scope

- No weaker diagnostic assertion, retry loop, fixed sleep, timeout inflation,
  test serialization, `--test-threads=1` acceptance, or suppression of a red run.
- No changes to dogfood health/UI polling policy, daemon shutdown response
  delivery, session-worker behavior, package manifests, app DTOs, public daemon
  protocol, WebRTC, SPA, TUI, Lua plugins, Rails relay, or Project Pipelines.
- No new process supervisor abstraction, async runtime, dependency, configurable
  output limit, diagnostic format, or public lifecycle state.
- No changes to the loaded lifecycle workflow, harness, or its documentation
  unless implementation proves the existing runner cannot exercise the final
  commit as designed. That would require a human scope decision.
- No adjacent cleanup in the large lifecycle test or supervisor module.

## Assumptions and unknowns

### Assumptions

- The ticket's required invariant is supervisor-owned ordering, not a CLI retry:
  every consumer of a terminal `EntrypointProcessSnapshot` should receive the
  same complete bounded diagnostics.
- Reader completion is part of terminal-state reconciliation. The smallest safe
  shape is to retain an observed `ExitStatus` internally while readers finish,
  and publish `failed`/`exited` only after both output channels are complete.
  This avoids blocking the daemon owner thread and avoids inventing a public
  `finalizing` state.
- While terminal output is finalizing, `start` must still regard the existing
  supervised process as owned so a second copy cannot spawn. The externally
  visible row may remain in its prior nonterminal state for this short internal
  phase, but it must not claim a terminal state with incomplete diagnostics.
- `OUTPUT_LIMIT_BYTES`, `bounded_message`, and the existing reader threads remain
  the source of bounding and path redaction. The change should coordinate their
  completion rather than add an unbounded synchronous pipe read.
- The final implementation commit can be dispatched through the existing loaded
  lifecycle workflow by exact SHA after it is pushed.

### Unknowns to resolve during implementation

- Whether the cleanest internal representation is a pending exit status on
  `SupervisedProcess` or a private finalizing enum variant. Choose the smaller
  private change; do not expose a new client-visible state.
- Whether stdout and stderr readers can complete on different refresh calls.
  The regression must cover one stream arriving after exit observation and
  require both streams to be settled before terminal publication.
- Whether a launched command can leave descendants holding inherited pipe file
  descriptors after the supervised child exits. The implementation must not add
  an unbounded join/receive on the daemon owner thread. If the current reader
  contract cannot signal bounded capture completion without blocking, stop and
  ask a human before changing process-group shutdown semantics.
- The captured flake frequency is not a success criterion. One deterministic
  red-when-reverted test plus the required loaded full-target campaign is the
  evidence boundary.

No human question blocks planning: the ticket names the exact missing output,
forbids assertion/retry workarounds, and the production race has one narrow
supervisor-owned interpretation. Any need to add a public lifecycle state,
change process-group semantics, weaken the diagnostic, or alter the loaded
campaign would be a scope-changing question rather than an implementation choice.

## Affected surfaces/files

- `src/entrypoint_supervisor.rs` — production fix and focused unit regression for
  exit/output reconciliation. Expected to be the only production code file.
- `tests/hub_daemon_lifecycle_test.rs` — preserve the existing exact real-dogfood
  assertion; touch only if a narrowly necessary barrier or additional assertion
  is required to prove exit 42 and stderr through the binary/socket path.
- `docs/plans/preserve-failed-dogfood-entrypoint-diagnostics-under-load.md` — this
  Plan-stage artifact.
- Read/execute only: `src/daemon_transport.rs`, `src/main.rs`, `test.sh`,
  `.github/workflows/loaded-daemon-lifecycle.yml`, and
  `script/run-loaded-daemon-lifecycle`.

Botster layer: Rust hub entrypoint supervisor, daemon package-status projection,
and CLI dogfood integration test. The daemon transport and CLI are production
entrypoints that prove the supervisor behavior is wired; they should not need
logic changes. The run is bound to the ticket's assigned target and worktree,
and loaded verification must bind the pushed implementation by exact commit SHA.

## Implementation plan

1. Add private pending-terminal bookkeeping to `SupervisedProcess`. On refresh,
   drain both reader channels, observe `try_wait` once, retain the `ExitStatus`,
   and continue draining on later refreshes without publishing the terminal
   state prematurely.
2. Finalize the retained exit only after stdout and stderr capture are complete:
   set `exited_at`, `exit_status`, process state, and structured launch-result
   state together. Preserve the exact existing diagnostic order, byte bound, and
   redaction behavior.
3. Audit `start`, `status`, `snapshots`, `stop`, `restart`, `stop_package`, and
   `stop_all` against the private finalizing phase. Prevent duplicate starts,
   preserve normal stop cleanup, and avoid any owner-thread blocking wait. Do not
   retrofit unrelated lifecycle behavior.
4. Add a deterministic unit regression in `src/entrypoint_supervisor.rs` that
   holds a reader sender open across exit observation, verifies no terminal
   snapshot is published yet, then delivers the exact stderr bytes and verifies
   the next snapshot is `failed`, `exit:42`, and contains the exact bounded
   `stderr` diagnostic. Cover both-stream completion if one channel settles
   earlier than the other.
5. Keep the existing dogfood lifecycle test's exact diagnostic assertion. Add
   only directly necessary exit-status/runtime-path coverage, then run it after
   explicitly building the session-worker binary so the focused command does not
   depend on another parallel test's setup.
6. Demonstrate the negative control: keep the new regression, temporarily revert
   only the production reconciliation change in an isolated copy or reversible
   patch, run the focused test, and record its nonzero result. Restore the fix and
   rerun green. A source diff is not red-when-reverted evidence.
7. Run formatting, strict lint, focused, lifecycle-target, and loaded campaign
   gates. Treat every first-root failure in the loaded default-parallel target as
   blocking unless base-versus-branch evidence supports a human re-scope; do not
   hide it with retries or a poisoned-lock cascade.

## Risks

- **Terminal status remains ahead of output.** A second nonblocking drain after
  `try_wait` is still scheduler-dependent. Mitigation: terminal publication must
  be gated on explicit reader completion, not another opportunistic poll.
- **Daemon owner thread blocks on inherited pipes.** Joining or receiving without
  a completion guarantee can hang status and shutdown. Mitigation: keep capture
  asynchronous and represent finalization privately; do not add an unbounded
  wait on the request path.
- **A finalizing process is accidentally restarted.** If `is_running` checks only
  the public state, a start request could duplicate ownership. Mitigation: make
  the internal pending-exit phase count as supervisor-owned until finalized.
- **Stop/restart drops the last bytes.** A stop request during finalization could
  overwrite state before readers drain. Mitigation: cover stop/restart control
  flow in the audit and add a focused assertion if the code path is not obviously
  preserved.
- **Output bounds or redaction regress.** Replacing the reader path could expose
  paths or unbounded output. Mitigation: reuse `OUTPUT_LIMIT_BYTES` and
  `bounded_message`; retain existing path-leak assertions in the dogfood test.
- **The integration test passes for the wrong reason.** A focused run can fail
  before entrypoint launch when the session worker is absent, or pass in
  isolation despite the race. Mitigation: build the worker explicitly, require
  the deterministic unit negative control, and inspect the exact asserted text.
- **Loaded acceptance exposes another root.** The lifecycle campaign is
  suite-wide by ticket intent. Mitigation: stop at the first red, isolate the
  first non-`PoisonError` panic, and fix it or ask a human after exact
  base-versus-branch attribution; do not silently waive it.
- **Scope spreads into CLI polling or workflow infrastructure.** Mitigation: the
  supervisor invariant is the fix boundary; `main.rs`, daemon transport, and the
  loaded runner are proof paths unless concrete evidence forces a question.

## Acceptance checks/tests

Use raw command output and exit statuses for all Rust gates.

1. Materialize the worker required by the exact dogfood launcher test:

   ```sh
   BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker
   ```

2. Run the new deterministic supervisor regression through the required wrapper
   using its exact Rust test name. It must pass with the fix, then return nonzero
   when only the production fix is reverted, then pass again after restoration:

   ```sh
   ./test.sh <exact-new-supervisor-regression-name> -- --exact --nocapture
   ```

3. Prove the real user/runtime path without changing its assertion:

   ```sh
   ./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics -- --exact --nocapture
   ```

   The output must show one passing test, and the exercised child contract remains
   stderr `bridge bind failed: fixture` plus exit 42. The test must continue to
   reject local checkout/package paths in the rendered diagnostic.

4. Run supervisor/package-entrypoint coverage and the complete lifecycle target
   at default Cargo parallelism:

   ```sh
   ./test.sh package_entrypoint_supervision -- --nocapture
   ./test.sh --test hub_daemon_lifecycle_test -- --nocapture
   ```

   Do not add `--test-threads=1`. If a lock is poisoned, identify the first
   non-`PoisonError` panic and preserve it as the failure evidence.

5. Run repository quality gates:

   ```sh
   cargo fmt --check
   cargo clippy --all-targets --all-features -- -D warnings
   git diff --check
   ```

6. After the implementation is committed and pushed, dispatch the exact final
   subject SHA through the existing isolated runner with
   `test_target=lifecycle-suite`, at least `20` repetitions, and
   `stress_profile=residual-tail`. The harness must execute
   `./test.sh --test hub_daemon_lifecycle_test -- --nocapture` at default
   parallelism, precompiled before load, and stop at the first red run. Attach
   the workflow URL and artifact metadata, run statuses, full logs, observed
   load samples, exact subject SHA, and cleanup evidence to Verify.

7. Acceptance is green only when the deterministic negative control is red on
   the reverted production change, all local gates pass, and every requested
   loaded lifecycle repetition passes. A missing stderr, a different first-root
   failure, timeout, cleanup failure, or fewer completed repetitions remains red
   unless a human explicitly changes scope.

## Pipeline gates and artifacts

- Plan gate: attach this document with all required fields and the completed
  vault checklist evidence.
- Plan Review: verify the supervisor—not CLI retries—owns the ordering invariant;
  reject blocking waits, public state additions, weakened assertions, or missing
  red-when-reverted/loaded proof.
- Implement: commit only the plan-traceable supervisor/test changes, attach a
  report separating behavior from any necessary merge cleanup, and link the PR.
- Review: inspect correctness, all terminal control paths, exact diagnostics,
  hidden blocking, scope, and the deterministic negative control using raw Cargo
  output.
- Verify: rerun local gates against the live worktree and require the exact-SHA
  loaded campaign artifact before approval.

## Vault gaps worth capturing

- The observed invariant is durable and not currently captured directly: a
  supervisor must not publish terminal process state before its bounded output
  capture has finalized. If implementation plus red-when-reverted proof confirms
  the private finalizing-state solution, capture one atomic gotcha through the
  vault inbox and connect it to [[botster runnable entrypoints are hub owned launch contracts]],
  [[installed apps are daemon app rows projected from package runnable entrypoints]],
  [[retention without a reachable flush is data loss]], [[botster-architecture]],
  and [[cli-patterns]].
- Capture a separate gotcha only if inherited pipe descriptors empirically force
  a process-group policy change. Do not record the current unknown as fact.
- No vault write is warranted during Plan. Implement/Verify should record the
  eventual inbox/capture path or explicitly state that no additional durable
  knowledge was discovered.

## Vault checklist evidence

- Notes read and constraints are recorded under **Context loaded**.
- Convention conflicts: none. The plan keeps ownership in the Rust hub
  supervisor, uses existing bounded readers and projections, drives the real
  dogfood entrypoint, uses `test.sh`, preserves default parallelism and exact
  assertions, and adds no dependency or speculative abstraction.
- Verification evidence: the planning probe and its missing-worker prerequisite
  are recorded above; implementation evidence must follow **Acceptance
  checks/tests**, including red-when-reverted and the existing loaded runner.
- Durable capture disposition: defer the identified candidate until the runtime
  invariant is verified; no Plan-stage vault capture was made.
