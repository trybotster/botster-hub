# Preserve failed dogfood entrypoint diagnostics under load

## Context loaded

- Project Pipelines context for ticket `ticket_1784168176_753693`, run
  `run_1784222795_280840`, active Plan step `botster_plan`, run step
  `run_step_1784223739_624518`, and gate `botster_plan_gate`. The first Plan
  artifact is `artifact_1784223280_960220`. Plan Review
  `review_1784223708_375099` returned changes required with three open findings:
  bound finalization when descendants retain pipe FDs, preserve the field-level
  `exited_at` contract, and update the existing one-shot failed-command test.
  There are no questions, answers, or blocking dependencies.
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
- Corroborating test evidence: `package_entrypoint_supervision_reports_failed_command`
  at `tests/hub_daemon_lifecycle_test.rs:9701` sleeps a fixed 100ms, performs one
  status request, then asserts `failed`, `exit:42`, and exact stderr. It is
  latently exposed to the same delayed-reader race and must observe the
  asynchronous terminal transition with bounded polling before retaining its
  exact diagnostic assertions.
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
  exit, unless a dedicated 500ms supervisor-internal finalization grace expires.
- Record the child-reaping facts (`exited_at`, `exit_status`, and absence of a
  live PID) immediately when `try_wait` observes exit. Gate only publication of
  the terminal `ProcessState` and structured launch-result process state.
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

- No weaker diagnostic assertion, retry that accepts a terminal row without the
  required diagnostic, fixed-sleep synchronization, timeout inflation, test
  serialization, `--test-threads=1` acceptance, or suppression of a red run.
- Bounded polling is permitted only to observe the asynchronous transition from
  running to terminal. Polling stops on the first terminal row, after which the
  existing exit-status and diagnostic assertions remain exact and immediate.
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
- Reader completion is part of terminal-state reconciliation, but it cannot be
  an unbounded gate because process-group descendants may inherit the write-side
  pipe FDs. The pinned shape is a private pending terminal state plus a dedicated
  `OUTPUT_FINALIZATION_GRACE` of 500ms. Publish `failed`/`exited` when both output
  channels complete or when that grace expires, whichever comes first. This
  avoids blocking the daemon owner thread, avoids inventing a public `finalizing`
  state, and cannot stall the dogfood health loop indefinitely.
- `exited_at` and `exit_status` are internal reaping facts, not publication
  gates. Set them immediately at `try_wait`; `snapshot()` must therefore report
  `pid: None` during private finalization, and `stop()` must retain its immediate
  already-exited fast path rather than spending `STOP_GRACE` signalling a dead
  group leader.
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

- Whether stdout and stderr readers can complete on different refresh calls.
  The regression must cover one stream arriving after exit observation and
  require both streams to be settled before terminal publication.
- The exact private field names remain an implementation detail, but their
  contracts are pinned: one pending terminal `ProcessState`, one monotonic
  finalization deadline, immediate `exited_at`/`exit_status`, and no public state
  addition.
- The captured flake frequency is not a success criterion. One deterministic
  red-when-reverted test plus the required loaded full-target campaign is the
  evidence boundary.

No human question blocks planning: the ticket names the exact missing output,
forbids assertion/retry workarounds, and the production race has one narrow
supervisor-owned interpretation. Any need to add a public lifecycle state,
change process-group semantics, weaken the diagnostic, change the pinned 500ms
supervisor grace, or alter the loaded campaign would be a scope-changing question
rather than an implementation choice.

## Affected surfaces/files

- `src/entrypoint_supervisor.rs` — production fix and focused unit regression for
  exit/output reconciliation. Expected to be the only production code file.
- `tests/hub_daemon_lifecycle_test.rs` — preserve the exact real-dogfood
  assertion and replace the fixed 100ms/one-shot observation in
  `package_entrypoint_supervision_reports_failed_command` with bounded polling
  for terminal-state arrival before its existing exact exit/stderr assertions.
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

1. Add private pending-terminal bookkeeping and a dedicated 500ms
   `OUTPUT_FINALIZATION_GRACE` to `SupervisedProcess`. On refresh, drain both
   reader channels, observe `try_wait` once, immediately record `exited_at` and
   `exit_status`, retain the pending terminal `ProcessState`, and start a
   monotonic finalization deadline.
2. Publish the retained terminal process state and structured launch-result
   state when both reader channels are complete or the deadline expires,
   whichever comes first. Preserve the exact existing diagnostic order, byte
   bound, and redaction behavior. Keep incomplete readers available for later
   nonblocking drains after deadline publication so late bounded bytes can still
   enrich later snapshots.
3. Audit `start`, `status`, `snapshots`, `stop`, `restart`, `stop_package`, and
   `stop_all` against the private finalizing phase. Prevent duplicate starts,
   preserve normal stop cleanup, and avoid any owner-thread blocking wait. Do not
   retrofit unrelated lifecycle behavior.
4. Add deterministic unit regressions in `src/entrypoint_supervisor.rs` that
   holds a reader sender open across exit observation, verifies no terminal
   snapshot is published yet, then delivers the exact stderr bytes and verifies
   the next snapshot is `failed`, `exit:42`, and contains the exact bounded
   `stderr` diagnostic. A second regression holds a sender open without sending,
   forces the private deadline-expiry path without a wall-clock sleep, and proves
   terminal state still publishes, `pid` is absent, and `stop()` does not spend
   `STOP_GRACE` treating the reaped child as live.
5. Replace the fixed 100ms sleep in
   `package_entrypoint_supervision_reports_failed_command` with bounded polling
   only while state remains `running`. Stop on the first terminal status, then
   keep the existing exact `failed`, `exit:42`, and stderr assertions unchanged.
   Keep the dogfood lifecycle test's exact diagnostic and path-redaction
   assertions unchanged, and run it after explicitly building the session-worker
   binary so the focused command does not depend on another parallel test's setup.
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
  a completion guarantee can hang status and shutdown, while even a nonblocking
  unbounded publication gate can stall state forever. Mitigation: keep capture
  asynchronous and cap private finalization at 500ms.
- **A finalizing process is accidentally restarted.** If `is_running` checks only
  the public state, a start request could duplicate ownership. Mitigation: make
  the internal pending-exit phase count as supervisor-owned until finalized.
- **Stop/restart drops the last bytes.** A stop request during finalization could
  overwrite state before readers drain. Mitigation: record reaping immediately,
  retain the stop fast path, keep late reader drains nonblocking, and assert the
  deadline path reports no dead PID or `STOP_GRACE` delay.
- **Existing one-shot test becomes a new load flake.** Deferring public state can
  leave the row running at its former 100ms observation point. Mitigation: poll
  with a finite test deadline only until terminal state arrives, then assert the
  exact diagnostic once; do not poll for diagnostic appearance after terminal.
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

   Run the deadline regression separately. With a reader sender deliberately
   left open and no bytes sent, it must publish the terminal state through the
   forced expiry branch, report no PID, and preserve fast stop behavior. The test
   should set the private monotonic deadline to expired rather than sleep 500ms.

3. Prove the real user/runtime path without changing its assertion:

   ```sh
   ./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_reports_failed_web_entrypoint_diagnostics -- --exact --nocapture
   ```

   The output must show one passing test, and the exercised child contract remains
   stderr `bridge bind failed: fixture` plus exit 42. The test must continue to
   reject local checkout/package paths in the rendered diagnostic.

4. Run `package_entrypoint_supervision_reports_failed_command` directly and prove
   its bounded state polling reaches one terminal row before asserting the exact
   exit 42 and stderr diagnostic. Then run supervisor/package-entrypoint coverage
   and the complete lifecycle target
   at default Cargo parallelism:

   ```sh
   ./test.sh --test hub_daemon_lifecycle_test package_entrypoint_supervision_reports_failed_command -- --exact --nocapture
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
  reject blocking or unbounded publication waits, public state additions,
  weakened assertions, polling after terminal state, or missing
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
  supervisor should delay terminal process publication for bounded output
  capture, while a deadline preserves liveness when descendants retain pipe FDs.
  If implementation plus red-when-reverted proof confirms the private bounded
  finalizing-state solution, capture one atomic gotcha through the
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
  assertions, permits bounded polling only for asynchronous state arrival, and
  adds no dependency or speculative abstraction.
- Verification evidence: the planning probe and its missing-worker prerequisite
  are recorded above; implementation evidence must follow **Acceptance
  checks/tests**, including red-when-reverted and the existing loaded runner.
- Durable capture disposition: defer the identified candidate until the runtime
  invariant is verified; no Plan-stage vault capture was made.
