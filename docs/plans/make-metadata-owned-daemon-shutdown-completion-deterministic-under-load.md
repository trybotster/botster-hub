# Make metadata-owned daemon shutdown completion deterministic under load

## Target and context loaded

- Ticket: `ticket_1785470554_126900`.
- Pipeline run/step: `run_1785475712_806874` / `botster_stack_plan`.
- Authoritative target: `botster-hub`, target
  `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through the Hub spawn-target
  registry rather than the ambient directory. The assigned worktree remote is
  `trybotster/botster-hub`.
- Base: `origin/main`. Before planning, the clean ticket branch was fetched and
  fast-forwarded from `868c61700c8c145e5dadca5005ae20ccf3220805` to current
  upstream `b1bca77a16c36276ffba6ea726b54ae0664e905b`.
- Repository playbook: [[botster-hub-playbook]].
- Role and surface playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and
  [[botster-runtime-verifier-playbook]].
- Required maps and planning notes: [[botster-architecture]], [[cli-patterns]],
  [[spa-patterns]],
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[plan agents must author vault context as wikilinks not home paths]],
  [[pipeline vault checklists must cite exact resolvable note titles]], and
  [[vault example paths are not repository placement conventions]].
- Hub ownership notes: [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[webrtc bootstrap origin must be requested after the package server binds]],
  [[plugin worker queue capacity and executor concurrency are independent host profile knobs]],
  and
  [[durable state version preflight must precede shape deserialization after cold turkey changes]].
- Targeted lifecycle/test notes:
  [[daemon shutdown disconnects count as success only after clean owned process exit]],
  [[worker shutdown completion requires lifecycle transport and process termination]],
  [[subprocess harnesses must kill child on failed readiness]],
  [[bounded command execution requires process group termination and reaping]],
  [[daemon probe order changes require lifecycle integration tests]],
  [[poisoned rust mutex test locks cascade one failure across parallel suite]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[workflow cancellation cleanup is idempotent across campaign traps and outer steps]],
  and [[sid scoped census is blind to setsid session leaks]].
- Repository evidence inspected: `README.md`, `Cargo.toml`, `test.sh`,
  `.github/workflows/loaded-daemon-lifecycle.yml`,
  `script/run-loaded-daemon-lifecycle`,
  `docs/loaded-daemon-lifecycle-runner.md`, prior lifecycle plans/reports,
  `src/main.rs`, `src/daemon_transport.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, and `tests/support/mod.rs`.
- Retained evidence inspected: GitHub Actions runs `30601821409` and
  `30570139134`, including the downloadable artifact for `30601821409`.
  Run `30601821409` passed repetition 1 and failed repetition 2 in the named
  test after `shutdown` returned success while PID `24852` still answered the
  alive-PID check. Its post-run token/SID census reported zero survivors and
  campaign cleanup status zero.
- The Project Pipelines repository playbook was not loaded: no Project
  Pipelines package/plugin file, pipeline product policy, or plugin operator
  surface is in scope. Project Pipelines is only the delivery mechanism.

Botster layer: Rust Hub CLI/local-daemon lifecycle plus its repository-owned
real-process integration and loaded CI proof. No SPA behavior is touched even
though the generic Botster planner map is required context.

## Current production-path finding

`botster-hub shutdown` and `down` already resolve the metadata-owned PID before
sending `DaemonShutdown`, receive and print the daemon response, then call
`complete_owned_runtime_daemon_shutdown`. That helper waits through
`wait_for_runtime_daemon_exit`, removes the configured socket, and removes the
metadata file. The operator command returns success only after that helper
returns.

The daemon-side response is a delivery acknowledgement, not process-exit
acknowledgement: `serve_daemon` waits for the connection task to attempt the
shutdown response, then stops the Hub runtime, removes the socket, returns
through `start_daemon`, and exits the process.

The suspect completion predicate is explicit in `src/main.rs`:
`wait_for_runtime_daemon_exit` returns success when `ps` reports either no PID
or a state beginning with `Z`. The test's `kill(pid, 0)` oracle still reports a
zombie PID as present. Under scheduler pressure, the interval between daemon
exit and adoption/reaping can therefore escape the production wait.

This is the leading code-backed hypothesis, not yet retained-run proof. The
existing artifact did not record PID `24852`'s state/PPID at the assertion
boundary. The first implementation action must characterize that boundary
before changing semantics. A live non-zombie state or PID identity change would
invalidate the zombie-only repair and requires a stop-and-replan rather than a
second speculative mechanism.

The loaded runner's current “zero survivors” result is not zero-zombie proof:
both SID and run-token census functions intentionally filter states beginning
with `Z`, and the runner documentation calls the outputs “non-zombie”
censuses.

## Scope

- Capture the exact PID state, PPID, PGID, SID, command identity, socket state,
  and metadata state at the controlled shutdown boundary.
- Add a deterministic real-process regression in which the test owns the real
  `botster-hub start` child, publishes its existing valid runtime metadata,
  invokes the real `botster-hub shutdown` CLI concurrently, observes the daemon
  in zombie state before reaping it, and proves shutdown remains pending until
  the owned child is reaped.
- If characterization confirms the current predicate is the defect, define
  metadata-owned daemon shutdown completion as disappearance of the recorded,
  identity-checked PID from the process table. Zombie is an exited process
  state but not completed/reaped ownership.
- Change the existing production wait predicate so `Z` remains pending inside
  the existing bounded wait; do not increase its ten-second budget.
- Keep socket and metadata removal after terminal PID disappearance. Preserve
  typed failure if the documented completion state is not reached within the
  existing budget.
- Preserve the current daemon response-delivery ordering and the CLI's
  post-response completion barrier for both `shutdown` and `down`.
- Add a focused loaded-runner selector for this exact lifecycle test and update
  the workflow input and runner documentation so focused branch/base campaigns
  do not depend on unrelated first-red tests.
- Make the loaded survivor evidence count and display zombies at the relevant
  ownership boundary. Keep the exact run-token plus SID/process topology and
  idempotent cleanup model; do not replace it with broad process-name killing.
- Document in `README.md` that successful `shutdown`/`down` for a verified
  metadata-owned runtime means the recorded PID is absent and its owned socket
  and metadata are removed.

## Non-scope

- No foreground-console progress, PTY handoff, foreground app, or console
  output change from the just-merged upstream ticket.
- No longer timeout, fixed sleep, blind retry, weakened `kill(pid, 0)`
  assertion, zombie-as-success rule, or test-only wait inserted after the CLI
  has returned.
- No daemon protocol/DTO revision, new shutdown response frame, Core contract,
  session-worker lifecycle, package supervision, plugin policy, or external
  client API change unless characterization disproves the local process-wait
  hypothesis.
- No new process supervisor, daemonization library, compatibility path, optional
  configuration, or broad subprocess abstraction.
- No cleanup of unrelated lifecycle code or retrofit of other process waits.
- No Project Pipelines package/plugin implementation change.

## Repository ownership and cross-repository dependencies

- `botster-hub` owns the changed behavior because the Hub charter assigns local
  control-plane topology, lifecycle, cleanup, supervision, daemon API
  composition, and host-profile policy to this repository.
- `botster-core` continues to own reusable session-worker and PTY mechanisms.
  The ticket does not require a Core dependency because the metadata-owned PID
  is the Hub binary spawned by `src/main.rs`, above the Core worker boundary.
- `botster-hub-client` owns external DTOs, but no DTO or compatibility change is
  planned. The existing `DaemonShutdown` request/response remains unchanged.
- Packages, Web, TUI, TUI kit, and Ghostty own no prerequisite for this repair.
- If characterization shows the surviving PID is actually a Core
  `botster-session-worker`, or completion requires a reusable child-reaping
  contract in Core, stop and register a dependency against the `botster-core`
  target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`; do not silently expand this run.

## Assumptions and unknowns

### Assumptions

- The recorded PID is still the same metadata-owned Hub process when the
  assertion fires. Existing command-line identity checks protect shutdown
  signalling and ownership selection.
- The repeated Linux failure is most likely the existing `Z => Ok(())`
  predicate becoming observable before PID 1 reaps the orphaned daemon. This is
  an inference from code and test semantics, not a claim supplied by the
  retained artifact.
- Waiting for process-table absence within the existing budget is lifecycle
  acknowledgement, not timeout inflation: success becomes stricter, while
  failure remains bounded and truthful.
- A regression that holds the real daemon as the integration test's unreaped
  direct child reproduces the exact kernel state deterministically without
  changing production code or relying on scheduler luck.
- The current response-delivery tests remain authoritative for the earlier
  transport acknowledgement boundary; this ticket adds the later
  process-terminal boundary.

### Unknowns and stop conditions

- The failed run did not retain `stat`, PPID, process start identity, or command
  for PID `24852` at the assertion. If a new controlled reproduction observes a
  non-zombie live Hub, an inspection-command failure, or PID reuse, stop and
  revise the plan around that evidence.
- Non-parent CLI processes cannot reap another process's zombie. The planned
  contract waits for the real owner/adoptive reaper to finish. If the existing
  ten-second budget proves insufficient on an authoritative environment,
  return a typed failure and ask the human before considering a persistent
  supervisor or daemonization topology change.
- Linux run-token lookup through `/proc/<pid>/environ` may not identify a
  zombie after its environment is gone. The Linux SID census must include
  zombies, and the focused test's exact PID/state evidence remains the primary
  oracle. Any cross-session survivor still needs the existing run-token
  mechanism or an exact-PID marker per [[sid scoped census is blind to setsid session leaks]].

## Affected surfaces/files

- `src/main.rs` — production metadata-owned PID selection, completion predicate,
  socket/metadata cleanup ordering, and focused private tests only if needed for
  state classification.
- `tests/hub_daemon_lifecycle_test.rs` — deterministic real-daemon
  zombie/reaping regression, retained ordinary `up`/`shutdown` assertion, exact
  PID state diagnostics, and cleanup guards.
- `script/run-loaded-daemon-lifecycle` — exact focused selector and
  zombie-inclusive survivor evidence.
- `.github/workflows/loaded-daemon-lifecycle.yml` — expose the focused selector.
- `docs/loaded-daemon-lifecycle-runner.md` — document focused campaign and
  zombie-inclusive evidence semantics.
- `README.md` — operator-facing successful shutdown completion contract.
- This plan and the later implementation report under the repository's
  established `docs/plans/` and `docs/reports/` hierarchy.

`src/daemon_transport.rs`, `crates/botster-hub-client`, and `botster-core` are
read-only wiring/ownership evidence unless characterization contradicts the
leading hypothesis.

## Implementation plan

1. Add bounded diagnostic helpers in the lifecycle integration test that read
   exact PID state and identity. On any boundary failure, print PID, PPID, PGID,
   SID, stat, command, metadata, socket, shutdown output/status, and daemon
   output/status. Do not add periodic production logging.
2. Add the deterministic regression using
   `start_cli_daemon_with_session_worker` plus
   `write_local_runtime_daemon_metadata`. Spawn the shutdown CLI without waiting
   immediately; wait semantically for the owned daemon child to reach `Z`;
   require the shutdown CLI still be running; reap the owned daemon; then
   require shutdown success, PID absence, metadata absence, and socket absence.
   Cleanup guards must reap both children on every assertion path.
3. Run the new test against current code before the production change. It must
   fail because the shutdown CLI returns while the daemon is zombie. Preserve
   the nonzero command output as pre-fix evidence.
4. Confirm the controlled state matches the retained failure's contract. If it
   does, remove only the `Z` success arm from
   `wait_for_runtime_daemon_exit`; success is `process_state(pid) == None`.
   Preserve the existing poll cadence and ten-second bound. Include the last
   observed state/identity in a typed timeout diagnostic if the current error
   cannot distinguish a live process from an unreaped zombie without widening
   the change.
5. Keep `complete_owned_runtime_daemon_shutdown` ordering unchanged:
   process-table disappearance, configured socket cleanup, then metadata
   cleanup. Confirm both `operator_shutdown` and `local_runtime_down` continue
   to call it after a successful protocol response.
6. Keep the existing ordinary metadata-owned `up`/`shutdown` integration test.
   Its immediate alive-PID assertion remains a second production-shaped oracle;
   do not replace it with the controlled-parent test.
7. Add `focused-metadata-owned-shutdown` in runner validation, command dispatch,
   workflow choices, and runner docs. Its command must use `./test.sh`, the exact
   integration test filter, `--exact`, `--nocapture`, and default Cargo test
   concurrency.
8. Remove the explicit zombie exclusions from the relevant Linux SID survivor
   census and document which evidence can and cannot attribute zombie rows.
   Keep run-token and process-group cleanup exact and idempotent. Add a
   runner-level fixture/self-check or artifact assertion showing a zombie row
   would make the gate nonzero; a prose-only change is insufficient.
9. Document the operator completion contract and produce an implementation
   report with characterization, pre-fix red, fixed green, exact subjects,
   branch/base campaigns, runtime binary provenance, and survivor evidence.

## Risks

- **Misdiagnosing an inferred zombie:** the retained artifact omitted PID state.
  Characterize first and stop if the observation differs.
- **Deadlocking the deterministic test:** fixed shutdown intentionally waits
  while the test owns an unreaped child. Use two child handles, semantic state
  barriers, bounded diagnostics, and explicit reap ordering.
- **Treating exit as reap:** `Z` proves the daemon executed exit but still owns a
  process-table entry. The ticket requires the later terminal state; preserve
  the alive-PID assertion.
- **Moving the race:** removing only the test assertion or adding a test wait
  after CLI completion would hide the production bug. The production CLI must
  remain blocked until the terminal predicate.
- **PID reuse/identity ambiguity:** capture command identity and do not signal or
  classify an unrelated reused PID as the owned daemon.
- **Overclaiming survivor proof:** `/proc` run-token scans may miss zombies.
  Require exact-PID regression evidence plus zombie-inclusive Linux SID/process
  evidence; do not describe the old zero-survivor artifacts as zero-zombie.
- **Runner cleanup unable to reap nonchildren:** detection should fail the
  campaign rather than pretend cleanup succeeded. Only an owning parent or PID
  1 can reap.
- **Upstream foreground overlap:** current main changed the same large
  integration test and runner. Limit edits to shutdown helpers/selectors and
  reject foreground-console cleanup.
- **Broad lifecycle refactor:** a new reaper/supervisor is disproportionate
  unless the bounded terminal wait is proven insufficient and the human
  approves the topology change.

## Acceptance checks and downstream proof

- Baseline after upstream sync:
  `./test.sh --test hub_daemon_lifecycle_test cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup -- --exact --nocapture`
  passed once at `b1bca77`; this is a local happy path, not resolution proof.
- Deterministic pre-fix red: the controlled-parent regression exits nonzero on
  unchanged production code because shutdown returns while the real daemon is
  `Z`. The enclosing wrapper status, exact PID diagnostics, and cleanup must be
  retained.
- Fixed deterministic green: the same test observes shutdown pending at `Z`,
  reaps the child, then observes successful shutdown, PID absence, socket
  absence, and metadata absence.
- Narrow ablation: restore only the old `Some(state) if state.starts_with('Z')
  => Ok(())` enforcement decision while retaining the committed test. The exact
  regression must fail; restore the fix and it must pass.
- Existing production path:
  `./test.sh --test hub_daemon_lifecycle_test cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup -- --exact --nocapture`.
- Adjacent lifecycle paths:
  `cli_local_runtime_up_starts_reuses_and_down_stops_runtime`,
  `cli_local_runtime_bootstrap_reuses_live_daemon_and_preserves_state_after_restart`,
  the operator-console shutdown test, and daemon response-delivery tests pass
  without changes to their response ordering or foreground behavior.
- Default-parallel integration binary:
  `./test.sh --test hub_daemon_lifecycle_test -- --nocapture`.
- Repository gates:
  `./test.sh --workspace --no-run`, `./test.sh --workspace`,
  `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and
  `git diff --check`.
- Focused loaded branch proof: dispatch the new exact selector for 20
  repetitions with `residual-tail` and default Cargo concurrency. Require every
  repetition green, exact Hub SHA and locked Core SHA, binary realpaths under
  the fresh target, zero Hub/session-worker/fixture-shell/socket survivors, no
  zombie rows, every owned group gone, and `cleanup_status=0`.
- Focused authoritative-base proof: run the identical selector, repetitions,
  stress profile, workflow harness, and runner image against the exact
  pre-fix/base SHA. Preserve a target red or explicitly report the bounded
  non-reproduction; do not retry away a red.
- Full downstream branch proof: run `full-suite-contention` for five
  `residual-tail` repetitions at default parallelism. No foreground-console red
  is attributed to this ticket without exact branch/base evidence.
- Full authoritative-base attribution: use identical full-suite inputs against
  the exact base SHA when any non-target red occurs. A base red does not waive a
  ticket-owned regression or missing zombie evidence.
- The implementation report must show the production entry points
  `operator_shutdown` and `local_runtime_down` using the changed terminal wait.
  A private helper/unit test alone is insufficient.

## Pipeline gates and artifacts

- Plan gate evidence must attach this repository-routed plan, exact target and
  playbook identities, upstream synchronization, assumptions/unknowns,
  ownership boundaries, affected files, risks, acceptance commands, and vault
  gaps.
- Implement evidence must attach the pre-fix red, fixed green, narrow ablation,
  characterization packet, exact diff, focused/adjacent commands, and the
  production call-chain proof.
- Verify evidence must independently rerun the deterministic regression,
  repository strict gates, focused loaded campaign, full contention campaign,
  exact branch/base attribution, and zombie-inclusive survivor checks.
- Review must reject timeout inflation, retry-only proof, weaker PID assertions,
  zombie filtering, missing child reaping, foreground-console edits, dead code,
  unwired helpers, or a new supervisor without a human-approved re-plan.

## Vault gaps worth capturing

- Candidate durable note after implementation:
  “metadata-owned daemon shutdown completes at PID disappearance not zombie
  observation.” Capture only after the deterministic negative control and
  loaded Linux proof establish the rule.
- Candidate runner gotcha after proof: run-token environment census may lose
  attribution after a process becomes zombie, so exact-PID or topology evidence
  must complement environment-based cleanup gates.
- No vault file should be written during Plan. If validated, route the capture
  through the inbox-first document/connect/verify workflow and link it to
  [[daemon shutdown disconnects count as success only after clean owned process exit]],
  [[bounded command execution requires process group termination and reaping]],
  [[a regression test must be shown to go red with the fix reverted]], and
  [[sid scoped census is blind to setsid session leaks]].
